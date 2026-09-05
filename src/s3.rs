//! S3 [`ObjectStore`] over blocking HTTP (`rusty-s3` signing, `ureq` transport).

use std::io::{self, Read};
use std::time::{Duration, Instant};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::store::{CondGet, CondPut, ObjectStore, Stored};
use crate::{StoreError, StoreOperation};

const SIGN_TTL: Duration = Duration::from_secs(300);

pub struct S3Config {
    /// e.g. `https://s3.us-east-1.amazonaws.com`, or a MinIO endpoint.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// Path-style addressing (`endpoint/bucket/key`); MinIO wants `true`,
    /// AWS virtual-hosted style wants `false`.
    pub path_style: bool,
}

/// Blocking transport budgets. The request deadline covers response headers,
/// body reads, and upload progress. DNS resolution and an already executing
/// transport/TLS call cannot be forcibly cancelled; an in-progress call can
/// overrun the nominal deadline. Uploads check it between bounded chunks.
/// These are whole-request deadlines, not independently reset idle timers.
#[derive(Clone, Debug)]
pub struct S3Options {
    /// Default: 10 seconds, capped to the request deadline.
    pub connect_timeout: Duration,
    /// Default: 60 seconds. Must be positive and below the signing TTL (300 s).
    pub request_timeout: Duration,
    /// Maximum body for both GET and PUT, including application payload objects.
    /// Default: 1 GiB. WalTier intersects its configured budgets with this limit.
    pub max_object_bytes: usize,
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(60),
            max_object_bytes: 1 << 30,
        }
    }
}

pub struct S3Store {
    bucket: Bucket,
    creds: Credentials,
    agent: ureq::Agent,
    namespace: String,
    opts: S3Options,
}

impl S3Store {
    pub fn new(cfg: S3Config) -> Result<Self, StoreError> {
        Self::new_with_options(cfg, S3Options::default())
    }

    pub fn new_with_options(cfg: S3Config, opts: S3Options) -> Result<Self, StoreError> {
        if opts.connect_timeout.is_zero()
            || opts.request_timeout.is_zero()
            || opts.request_timeout >= SIGN_TTL
            || opts.max_object_bytes == 0
            || opts.max_object_bytes > isize::MAX as usize
        {
            return Err(StoreError::new(
                "S3 timeouts/body budget must be positive; request timeout must be below 300 seconds and body budget must fit isize",
            ).not_applied());
        }
        let endpoint: url::Url = cfg
            .endpoint
            .parse()
            .map_err(|e| StoreError::new(format!("bad endpoint: {e}")).not_applied())?;
        let style = if cfg.path_style {
            UrlStyle::Path
        } else {
            UrlStyle::VirtualHost
        };
        let namespace = format!(
            "s3:{}:{}:{}:{}:{}:{}:{}:{}",
            endpoint.as_str().len(),
            endpoint,
            cfg.bucket.len(),
            cfg.bucket,
            cfg.access_key.len(),
            cfg.access_key,
            cfg.region,
            cfg.path_style,
        );
        let bucket = Bucket::new(endpoint, style, cfg.bucket, cfg.region)
            .map_err(|e| StoreError::new(format!("bad bucket config: {e}")).not_applied())?;
        let creds = Credentials::new(cfg.access_key, cfg.secret_key);
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(opts.connect_timeout.min(opts.request_timeout))
            .timeout(opts.request_timeout)
            // A redirected mutation must never be interpreted as an accepted PUT.
            .redirects(0)
            .build();
        Ok(Self {
            bucket,
            creds,
            namespace,
            agent,
            opts,
        })
    }

    fn read_body(&self, key: &str, resp: ureq::Response) -> Result<Vec<u8>, StoreError> {
        let status = resp.status();
        if resp
            .header("Content-Length")
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|n| n > self.opts.max_object_bytes as u64)
        {
            return Err(store_error(
                StoreOperation::Get,
                key,
                Some(status),
                "body exceeds byte limit",
            ));
        }
        let mut buf = Vec::new();
        resp.into_reader()
            .take(self.opts.max_object_bytes as u64 + 1)
            .read_to_end(&mut buf)
            .map_err(|e| store_error(StoreOperation::Get, key, Some(status), e.to_string()))?;
        if buf.len() > self.opts.max_object_bytes {
            return Err(store_error(
                StoreOperation::Get,
                key,
                Some(status),
                "body exceeds byte limit",
            ));
        }
        Ok(buf)
    }

    fn send_put(
        &self,
        key: &str,
        etag: Option<Option<&str>>,
        data: &[u8],
    ) -> Result<ureq::Response, StoreError> {
        if data.len() > self.opts.max_object_bytes {
            return Err(
                store_error(StoreOperation::Put, key, None, "body exceeds byte limit")
                    .not_applied(),
            );
        }
        let url = self
            .bucket
            .put_object(Some(&self.creds), key)
            .sign(SIGN_TTL);
        let mut req = self
            .agent
            .request_url("PUT", &url)
            .set("Content-Length", &data.len().to_string());
        if let Some(condition) = etag {
            req = match condition {
                Some(e) => req.set("If-Match", e),
                None => req.set("If-None-Match", "*"),
            };
        }
        let upload = Upload {
            remaining: data,
            deadline: Instant::now() + self.opts.request_timeout,
        };
        match req.send(upload) {
            Ok(resp) => Ok(resp),
            Err(ureq::Error::Status(_, resp)) => Ok(resp),
            Err(e) => Err(store_error(StoreOperation::Put, key, None, e.to_string())),
        }
    }
}

/// ureq 2 updates the deadline while reading responses, but its upload uses
/// socket timeouts. Check elapsed time between chunks as well so a steadily
/// progressing upload cannot run without ever examining its deadline.
struct Upload<'a> {
    remaining: &'a [u8],
    deadline: Instant,
}

impl Read for Upload<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "upload deadline exceeded",
            ));
        }
        let count = buf.len().min(self.remaining.len()).min(8192);
        buf[..count].copy_from_slice(&self.remaining[..count]);
        self.remaining = &self.remaining[count..];
        Ok(count)
    }
}

fn store_error(
    op: StoreOperation,
    key: &str,
    status: Option<u16>,
    message: impl std::fmt::Display,
) -> StoreError {
    let error = StoreError::new(format!("{op:?} {key}: {message}")).with_context(op, key, status);
    if op == StoreOperation::Get {
        error.not_applied()
    } else {
        error
    }
}

fn etag_of(op: StoreOperation, key: &str, resp: &ureq::Response) -> Result<String, StoreError> {
    resp.header("etag")
        .filter(|e| !e.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            store_error(
                op,
                key,
                Some(resp.status()),
                "response is missing an ETag header",
            )
        })
}

impl ObjectStore for S3Store {
    fn cache_namespace(&self) -> Option<String> {
        Some(self.namespace.clone())
    }
    fn max_object_bytes(&self) -> Option<usize> {
        Some(self.opts.max_object_bytes)
    }

    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        match self.get_if_changed(key, None)? {
            CondGet::Changed(s) => Ok(Some(s)),
            CondGet::Missing => Ok(None),
            CondGet::NotModified => unreachable!("unconditional 304 is rejected below"),
        }
    }

    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        let url = self
            .bucket
            .get_object(Some(&self.creds), key)
            .sign(SIGN_TTL);
        let mut req = self.agent.request_url("GET", &url);
        if let Some(etag) = etag {
            req = req.set("If-None-Match", etag);
        }
        let resp = match req.call() {
            Ok(resp) | Err(ureq::Error::Status(_, resp)) => resp,
            Err(e) => return Err(store_error(StoreOperation::Get, key, None, e.to_string())),
        };
        match resp.status() {
            304 if etag.is_some() => Ok(CondGet::NotModified),
            304 => Err(store_error(
                StoreOperation::Get,
                key,
                Some(304),
                "unexpected 304 to unconditional GET",
            )),
            404 => Ok(CondGet::Missing),
            200 => Ok(CondGet::Changed(Stored {
                etag: etag_of(StoreOperation::Get, key, &resp)?,
                data: self.read_body(key, resp)?,
            })),
            status => Err(store_error(
                StoreOperation::Get,
                key,
                Some(status),
                format!("unexpected HTTP status {status}"),
            )),
        }
    }

    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        let resp = self.send_put(key, Some(etag), data)?;
        match resp.status() {
            200 => Ok(CondPut::Ok {
                etag: etag_of(StoreOperation::Put, key, &resp)?,
            }),
            // Both statuses reject this conditional attempt. Refresh before retry.
            409 | 412 => Ok(CondPut::PreconditionFailed),
            status => Err(store_error(
                StoreOperation::Put,
                key,
                Some(status),
                format!("unexpected HTTP status {status}"),
            )),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        let resp = self.send_put(key, None, data)?;
        if resp.status() == 200 {
            etag_of(StoreOperation::Put, key, &resp)
        } else {
            Err(store_error(
                StoreOperation::Put,
                key,
                Some(resp.status()),
                format!("unexpected HTTP status {}", resp.status()),
            ))
        }
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let url = self
            .bucket
            .delete_object(Some(&self.creds), key)
            .sign(SIGN_TTL);
        match self.agent.request_url("DELETE", &url).call() {
            Ok(resp) if (200..300).contains(&resp.status()) => Ok(()),
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Ok(resp) | Err(ureq::Error::Status(_, resp)) => Err(store_error(
                StoreOperation::Delete,
                key,
                Some(resp.status()),
                format!("unexpected HTTP status {}", resp.status()),
            )),
            Err(e) => Err(store_error(
                StoreOperation::Delete,
                key,
                None,
                e.to_string(),
            )),
        }
    }
}
