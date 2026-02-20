use std::fmt::Display;
use std::sync::Arc;

use crate::file::FileReader;
use async_trait::async_trait;
use datafusion::object_store::path::Path;
use datafusion::object_store::{self, *};
use futures::stream::BoxStream;

#[derive(Debug)]
pub struct JsObjectStore(Arc<[FileReader]>);

impl JsObjectStore {
    pub fn new(files: Arc<[FileReader]>) -> Self {
        Self(files)
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
        let file = parse_url(location, &self.0)?;

        let size = file.size();
        let range = match options.range {
            Some(range) => range.as_range(size).unwrap(), // FIXME
            None => 0..size,
        };

        let payload = {
            let mut file = file.clone();
            let range = range.clone();
            GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
                Ok(file.read(range).await.into())
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

fn parse_url<'a>(location: &Path, files: &'a [FileReader]) -> Result<&'a FileReader> {
    let err = || object_store::Error::Generic {
        store: "JsFileStore",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid path: {}", location),
        )),
    };

    let filename = location.parts().next().ok_or_else(err)?;
    let idx = filename.as_ref().split('.').next().ok_or_else(err)?;
    let idx = idx.parse::<usize>().ok().ok_or_else(err)?;
    let file = files.get(idx).ok_or_else(err)?;
    // let encoding = parts.next().map(|s| s.as_ref().to_string());

    Ok(file)
}
