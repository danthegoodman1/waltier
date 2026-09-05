//! S3 [`ObjectStore`] over blocking HTTP (`rusty-s3` for signing, `ureq` for
//! transport). Requires a store with conditional writes: S3 general-purpose
//! buckets support `If-Match` / `If-None-Match` PUTs in all regions.

use std::io::Read;
use std::time::Duration;

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::StoreError;
use crate::store::{CondGet, CondPut, ObjectStore, Stored};

const SIGN_TTL: Duration = Duration::from_secs(300);
/// Refuse bodies over this size rather than buffering without bound. The WAL
/// image and snapshots are expected to stay far below it.
const MAX_BODY: u64 = 1 << 30;

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

pub struct S3Store {
    bucket: Bucket,
    creds: Credentials,
    agent: ureq::Agent,
    namespace: String,
}

impl S3Store {
    pub fn new(cfg: S3Config) -> Result<Self, StoreError> {
        let endpoint: url::Url = cfg
            .endpoint
            .parse()
            .map_err(|e| StoreError(format!("bad endpoint {}: {e}", cfg.endpoint)))?;
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
            cfg.path_style
        );
        let bucket = Bucket::new(endpoint, style, cfg.bucket, cfg.region)
            .map_err(|e| StoreError(format!("bad bucket config: {e}")))?;
        let creds = Credentials::new(cfg.access_key, cfg.secret_key);
        Ok(Self {
            bucket,
            creds,
            namespace,
            agent: ureq::AgentBuilder::new().build(),
        })
    }
}

fn etag_of(resp: &ureq::Response) -> Result<String, StoreError> {
    resp.header("etag")
        .map(str::to_owned)
        .ok_or_else(|| StoreError("response is missing an ETag header".into()))
}

fn read_body(key: &str, resp: ureq::Response) -> Result<Vec<u8>, StoreError> {
    let mut buf = Vec::new();
    // One byte past the limit, so an oversized body is refused rather than
    // silently truncated into something the app would try to restore.
    resp.into_reader()
        .take(MAX_BODY + 1)
        .read_to_end(&mut buf)
        .map_err(|e| StoreError(format!("read body of {key}: {e}")))?;
    if buf.len() as u64 > MAX_BODY {
        return Err(StoreError(format!(
            "GET {key}: body exceeds the {MAX_BODY} byte limit"
        )));
    }
    Ok(buf)
}

impl ObjectStore for S3Store {
    fn cache_namespace(&self) -> Option<String> {
        Some(self.namespace.clone())
    }

    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        match self.get_if_changed(key, None)? {
            CondGet::Changed(s) => Ok(Some(s)),
            CondGet::Missing => Ok(None),
            CondGet::NotModified => Err(StoreError(format!(
                "GET {key}: unexpected 304 to an unconditional request"
            ))),
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
        match req.call() {
            Ok(resp) if resp.status() == 304 => Ok(CondGet::NotModified),
            Ok(resp) => {
                let etag = etag_of(&resp)?;
                Ok(CondGet::Changed(Stored {
                    etag,
                    data: read_body(key, resp)?,
                }))
            }
            Err(ureq::Error::Status(304, _)) => Ok(CondGet::NotModified),
            Err(ureq::Error::Status(404, _)) => Ok(CondGet::Missing),
            Err(e) => Err(StoreError(format!("GET {key}: {e}"))),
        }
    }

    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        let url = self
            .bucket
            .put_object(Some(&self.creds), key)
            .sign(SIGN_TTL);
        let req = self.agent.request_url("PUT", &url);
        let req = match etag {
            Some(e) => req.set("If-Match", e),
            None => req.set("If-None-Match", "*"),
        };
        match req.send_bytes(data) {
            Ok(resp) => Ok(CondPut::Ok {
                etag: etag_of(&resp)?,
            }),
            // 412: precondition failed. 409: S3's ConditionalRequestConflict,
            // raised when concurrent conditional writes collide mid-flight;
            // treating it as a lost CAS makes the caller re-pull and retry.
            Err(ureq::Error::Status(412 | 409, _)) => Ok(CondPut::PreconditionFailed),
            Err(e) => Err(StoreError(format!("PUT {key}: {e}"))),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        let url = self
            .bucket
            .put_object(Some(&self.creds), key)
            .sign(SIGN_TTL);
        match self.agent.request_url("PUT", &url).send_bytes(data) {
            Ok(resp) => etag_of(&resp),
            Err(e) => Err(StoreError(format!("PUT {key}: {e}"))),
        }
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let url = self
            .bucket
            .delete_object(Some(&self.creds), key)
            .sign(SIGN_TTL);
        match self.agent.request_url("DELETE", &url).call() {
            Ok(_) | Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(e) => Err(StoreError(format!("DELETE {key}: {e}"))),
        }
    }
}
