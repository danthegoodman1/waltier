#![cfg(feature = "s3")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use waltier::{
    CondGet, CondPut, MutationOutcome, ObjectStore, S3Config, S3Options, S3Store, StoreError,
    StoreOperation,
};

fn config(endpoint: String) -> S3Config {
    S3Config {
        endpoint,
        region: "local".into(),
        bucket: "bucket".into(),
        access_key: "test-key".into(),
        secret_key: "test-secret".into(),
        path_style: true,
    }
}

/// Each script has a bounded accept, socket timeout, and finite lifetime even
/// when a client assertion fails or a request is refused before transport.
fn server(script: impl FnOnce(TcpStream) + Send + 'static) -> (S3Config, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let cfg = config(format!("http://{}", listener.local_addr().unwrap()));
    listener.set_nonblocking(true).unwrap();
    let task = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(1))
                }
                other => panic!("server accept failed: {other:?}"),
            }
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        script(socket);
    });
    (cfg, task)
}

fn headers(socket: &mut TcpStream) -> String {
    let mut result = vec![];
    while !result.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte).unwrap();
        result.push(byte[0]);
        assert!(result.len() < 16 * 1024);
    }
    String::from_utf8(result).unwrap().to_ascii_lowercase()
}
fn body(socket: &mut TcpStream, headers: &str) -> Vec<u8> {
    let n: usize = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length: "))
        .unwrap()
        .parse()
        .unwrap();
    let mut body = vec![0; n];
    socket.read_exact(&mut body).unwrap();
    body
}
fn response(socket: &mut TcpStream, status: u16, etag: bool, data: &[u8]) {
    let etag = if etag { "ETag: \"version\"\r\n" } else { "" };
    write!(
        socket,
        "HTTP/1.1 {status} Scripted\r\n{etag}Content-Length: {}\r\nConnection: close\r\n\r\n",
        data.len()
    )
    .unwrap();
    socket.write_all(data).unwrap();
}
fn short_options() -> S3Options {
    S3Options {
        request_timeout: Duration::from_millis(80),
        connect_timeout: Duration::from_millis(80),
        ..S3Options::default()
    }
}
fn assert_unknown_put(error: &StoreError, status: Option<u16>) {
    assert_eq!(error.operation, Some(StoreOperation::Put));
    assert_eq!(error.key.as_deref(), Some("wal"));
    assert_eq!(error.status, status);
    assert_eq!(error.mutation_outcome, MutationOutcome::Unknown);
}

#[test]
fn conditional_put_headers_and_conflict_statuses() {
    for (status, expected) in [(200, None), (409, Some("old")), (412, Some("old"))] {
        let (cfg, task) = server(move |mut socket| {
            let h = headers(&mut socket);
            assert!(h.starts_with("put /bucket/wal?"));
            assert!(!h.contains("transfer-encoding:"));
            match expected {
                None => assert!(h.contains("if-none-match: *\r\n")),
                Some(_) => assert!(h.contains("if-match: old\r\n")),
            }
            assert_eq!(body(&mut socket, &h), b"data");
            response(&mut socket, status, true, b"");
        });
        let store = S3Store::new(cfg).unwrap();
        let result = store.put_if_match("wal", expected, b"data").unwrap();
        assert_eq!(matches!(result, CondPut::Ok { .. }), status == 200);
        task.join().unwrap();
    }
}

#[test]
fn conditional_get_304_404_and_valid_body() {
    for (status, validator) in [(200, None), (304, Some("old")), (404, None)] {
        let (cfg, task) = server(move |mut socket| {
            let h = headers(&mut socket);
            assert!(h.starts_with("get /bucket/wal?"));
            assert_eq!(h.contains("if-none-match: old\r\n"), validator.is_some());
            response(
                &mut socket,
                status,
                true,
                if status == 200 { b"data" } else { b"" },
            );
        });
        let store = S3Store::new(cfg).unwrap();
        match (status, store.get_if_changed("wal", validator).unwrap()) {
            (200, CondGet::Changed(s)) => {
                assert_eq!(s.data, b"data");
                assert_eq!(s.etag, "\"version\"");
            }
            (304, CondGet::NotModified) | (404, CondGet::Missing) => {}
            result => panic!("wrong result: {result:?}"),
        }
        task.join().unwrap();
    }
}

#[test]
fn unconditional_304_and_missing_etags_are_contextual_errors() {
    for (method, status) in [
        ("GET", 304),
        ("GET", 200),
        ("PUT", 200),
        ("PUT", 500),
        ("PUT", 302),
    ] {
        let (cfg, task) = server(move |mut socket| {
            let h = headers(&mut socket);
            if method == "PUT" {
                body(&mut socket, &h);
            }
            response(&mut socket, status, false, b"");
        });
        let store = S3Store::new(cfg).unwrap();
        let error = if method == "GET" {
            store.get_if_changed("wal", None).unwrap_err()
        } else {
            store.put_if_match("wal", Some("old"), b"data").unwrap_err()
        };
        assert_eq!(error.status, Some(status));
        assert_eq!(error.key.as_deref(), Some("wal"));
        if method == "PUT" {
            assert_unknown_put(&error, Some(status));
        } else {
            assert_eq!(error.mutation_outcome, MutationOutcome::NotApplied);
        }
        task.join().unwrap();
    }
}

#[test]
fn body_limits_apply_to_known_unknown_get_lengths_and_all_puts() {
    for known_length in [true, false] {
        let (cfg, task) = server(move |mut socket| {
            headers(&mut socket);
            let length = if known_length {
                "Content-Length: 5\r\n"
            } else {
                ""
            };
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nETag: e\r\n{length}Connection: close\r\n\r\nabcde"
            )
            .unwrap();
        });
        let opts = S3Options {
            max_object_bytes: 4,
            ..S3Options::default()
        };
        let store = S3Store::new_with_options(cfg, opts).unwrap();
        let error = store.get("wal").unwrap_err();
        assert_eq!(error.status, Some(200));
        assert!(error.message.contains("byte limit"));
        task.join().unwrap();
    }
    // An unreachable endpoint demonstrates rejection occurs before any request.
    let opts = S3Options {
        max_object_bytes: 4,
        ..S3Options::default()
    };
    let store = S3Store::new_with_options(config("http://127.0.0.1:1".into()), opts).unwrap();
    for error in [
        store.put("wal", b"abcde").unwrap_err(),
        store.put_if_match("wal", None, b"abcde").unwrap_err(),
    ] {
        assert_eq!(error.mutation_outcome, MutationOutcome::NotApplied);
        assert_eq!(error.status, None);
        assert!(error.message.contains("byte limit"));
    }
}

#[test]
fn stalled_response_headers_and_body_obey_deadline() {
    for send_headers in [false, true] {
        let (cfg, task) = server(move |mut socket| {
            headers(&mut socket);
            if send_headers {
                socket.write_all(b"HTTP/1.1 200 OK\r\nETag: e\r\nContent-Length: 2\r\nConnection: close\r\n\r\na").unwrap();
            }
            thread::sleep(Duration::from_millis(400));
        });
        let store = S3Store::new_with_options(cfg, short_options()).unwrap();
        let start = Instant::now();
        let error = store.get("wal").unwrap_err();
        assert!(
            start.elapsed() < Duration::from_millis(350),
            "deadline was not enforced: {error}"
        );
        assert_eq!(error.operation, Some(StoreOperation::Get));
        assert_eq!(error.status, if send_headers { Some(200) } else { None });
        task.join().unwrap();
    }
}

#[test]
fn successful_upload_with_lost_response_is_unknown() {
    let (cfg, task) = server(|mut socket| {
        let h = headers(&mut socket);
        assert_eq!(body(&mut socket, &h), b"accepted");
        thread::sleep(Duration::from_millis(400));
    });
    let store = S3Store::new_with_options(cfg, short_options()).unwrap();
    let start = Instant::now();
    let error = store
        .put_if_match("wal", Some("old"), b"accepted")
        .unwrap_err();
    assert!(start.elapsed() < Duration::from_millis(350));
    assert_unknown_put(&error, None);
    task.join().unwrap();
}

#[test]
fn stalled_upload_is_bounded_and_unknown() {
    let (cfg, task) = server(|mut socket| {
        headers(&mut socket);
        // Never read the request body: a 16 MiB body exceeds the TCP buffers.
        thread::sleep(Duration::from_millis(600));
    });
    let store = S3Store::new_with_options(cfg, short_options()).unwrap();
    let payload = vec![0; 16 << 20];
    let start = Instant::now();
    let error = store.put_if_match("wal", None, &payload).unwrap_err();
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "stalled upload was not bounded: {error}"
    );
    assert_unknown_put(&error, None);
    task.join().unwrap();
}

#[test]
fn deletion_accepts_missing_and_preserves_error_context() {
    for status in [204, 404, 503] {
        let (cfg, task) = server(move |mut socket| {
            assert!(headers(&mut socket).starts_with("delete /bucket/wal?"));
            response(&mut socket, status, false, b"");
        });
        let result = S3Store::new(cfg).unwrap().delete("wal");
        if status == 503 {
            let error = result.unwrap_err();
            assert_eq!(error.operation, Some(StoreOperation::Delete));
            assert_eq!(error.status, Some(503));
            assert_eq!(error.mutation_outcome, MutationOutcome::Unknown);
        } else {
            result.unwrap();
        }
        task.join().unwrap();
    }
}

#[test]
fn options_reject_unbounded_and_invalid_values() {
    for invalid in 0..4 {
        let mut opts = S3Options::default();
        match invalid {
            0 => opts.connect_timeout = Duration::ZERO,
            1 => opts.request_timeout = Duration::ZERO,
            2 => opts.request_timeout = Duration::from_secs(300),
            _ => opts.max_object_bytes = 0,
        }
        assert!(S3Store::new_with_options(config("http://127.0.0.1:1".into()), opts).is_err());
    }
    assert_eq!(
        StoreError::new("custom timeout").mutation_outcome,
        MutationOutcome::Unknown
    );
}
