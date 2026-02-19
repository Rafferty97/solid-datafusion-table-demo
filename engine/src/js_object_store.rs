use std::convert::TryFrom;
use std::fmt::Display;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::object_store::path::Path;
use datafusion::object_store::{self, *};
use futures::channel::{mpsc, oneshot};
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt};
use wasm_bindgen_futures::JsFuture;

#[derive(Debug)]
pub struct JsObjectStore(Arc<[File]>);

impl JsObjectStore {
    pub fn new(files: Arc<[File]>) -> Self {
        Self(files)
    }
}

#[derive(Clone, Debug)]
pub struct File {
    size: u64,
    read_bytes: mpsc::UnboundedSender<(Range<u64>, oneshot::Sender<Vec<u8>>)>,
    // FIXME: Add `Option<Arc<dyn Decoder>>`
}

impl File {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let size = bytes.len() as u64;
        let (read_bytes, mut read_bytes_rx) = mpsc::unbounded();
        let out = Self { size, read_bytes };

        wasm_bindgen_futures::spawn_local(async move {
            while let Some((range, tx)) = read_bytes_rx.next().await {
                let bytes = bytes[range.start as usize..range.end as usize].to_vec();
                tx.send(bytes).unwrap();
            }
        });

        out
    }

    pub fn from_file(file: web_sys::File) -> Self {
        let size = file.size() as u64;
        let (read_bytes, mut read_bytes_rx) = mpsc::unbounded();
        let out = Self { size, read_bytes };

        wasm_bindgen_futures::spawn_local(async move {
            while let Some((range, tx)) = read_bytes_rx.next().await {
                // let src_range = decoder
                //     .as_ref()
                //     .map(|d| d.calc_input_range(range.clone()))
                //     .unwrap_or(range.clone());
                let src_range = range.clone();
                let bytes = file
                    .slice_with_i32_and_i32(src_range.start as i32, src_range.end as i32)
                    .unwrap();
                let bytes = JsFuture::from(bytes.array_buffer()).await.unwrap();
                let bytes = js_sys::Uint8Array::new(&bytes).to_vec();
                // let bytes = decoder
                //     .as_ref()
                //     .map(|d| d.decode_range(&bytes, src_range.start, range))
                //     .unwrap_or(bytes);
                tx.send(bytes).unwrap();
            }
        });

        out
    }

    pub async fn from_file_handle(handle: web_sys::FileSystemFileHandle) -> Self {
        let file = JsFuture::from(handle.get_file()).await.unwrap();
        let file = web_sys::File::try_from(file).unwrap();
        Self::from_file(file)
    }

    pub fn size(&self) -> u64 {
        self.size
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

        let payload: GetResultPayload = {
            let mut file = file.clone();
            let range = range.clone();
            GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
                let (tx, rx) = futures::channel::oneshot::channel::<Vec<u8>>();
                // wasm_bindgen_futures::spawn_local(async move {
                //     let buffer = if let Some(decoder) = &decoder {
                //         let src_range = decoder.calc_input_range(range.clone());
                //         let buffer = js_file.read_bytes(src_range.clone()).await;
                //         let result = decoder.decode_range(&buffer, src_range.start, range);
                //         result
                //     } else {
                //         js_file.read_bytes(range).await
                //     };
                //     tx.send(buffer.to_vec()).expect("rx was dropped");
                // });
                file.read_bytes.send((range, tx)).await.expect("rx was dropped");
                let out = rx.await.expect("failed to read data");
                Ok(out.into())
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

    async fn put_opts(&self, _location: &Path, _payload: PutPayload, _opts: PutOptions) -> Result<PutResult> {
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

    fn list_with_offset(&self, _prefix: Option<&Path>, _offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
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

fn parse_url<'a>(location: &Path, files: &'a [File]) -> Result<&'a File> {
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
