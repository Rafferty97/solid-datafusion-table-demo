use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::object_store::path::Path;
use datafusion::object_store::*;
use futures::stream::BoxStream;

#[derive(Debug)]
pub struct JsObjectStore(Arc<[u8]>);

impl JsObjectStore {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes.into())
    }
}

impl Display for JsObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsObjectStore")
    }
}

#[async_trait]
impl ObjectStore for JsObjectStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let size = self.0.len() as u64;
        let range = match options.range {
            Some(range) => range.as_range(size).unwrap(), // FIXME
            None => 0..size,
        };

        let payload = {
            let bytes = self.0.clone();
            let range = range.clone();
            GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
                Ok(bytes[range.start as usize..range.end as usize]
                    .to_vec()
                    .into())
            })))
        };

        Ok(GetResult {
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            payload,
            range,
            attributes: Default::default(),
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        unimplemented!()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        unimplemented!()
    }

    fn list_with_offset(
        &self,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        unimplemented!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        unimplemented!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }
}
