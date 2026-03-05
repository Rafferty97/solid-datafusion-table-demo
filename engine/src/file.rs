use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::Range;

use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};

pub trait FileSource {
    fn size(&self) -> u64;

    async fn read(&self, range: Range<u64>) -> Vec<u8>;
}

impl FileSource for Vec<u8> {
    fn size(&self) -> u64 {
        self.len().try_into().expect("length exceeds u64::MAX")
    }

    async fn read(&self, range: Range<u64>) -> Vec<u8> {
        self[range.start as usize..range.end as usize].to_vec()
    }
}

impl FileSource for js_sys::Uint8Array {
    fn size(&self) -> u64 {
        self.byte_length().into()
    }

    async fn read(&self, range: Range<u64>) -> Vec<u8> {
        let start = range.start.try_into().expect("start exceeds u32::MAX");
        let end = range.end.try_into().expect("end exceeds u32::MAX");
        self.slice(start, end).to_vec()
    }
}

impl FileSource for web_sys::Blob {
    fn size(&self) -> u64 {
        self.size() as _
    }

    async fn read(&self, range: Range<u64>) -> Vec<u8> {
        use wasm_bindgen_futures::JsFuture;
        let start = range.start.try_into().expect("start exceeds u32::MAX");
        let end = range.end.try_into().expect("end exceeds u32::MAX");
        let bytes = self.slice_with_i32_and_i32(start, end).unwrap();
        let bytes = JsFuture::from(bytes.array_buffer()).await.unwrap();
        js_sys::Uint8Array::new(&bytes).to_vec()
    }
}

impl FileSource for web_sys::File {
    fn size(&self) -> u64 {
        FileSource::size(self as &web_sys::Blob)
    }

    async fn read(&self, range: Range<u64>) -> Vec<u8> {
        FileSource::read(self as &web_sys::Blob, range).await
    }
}

#[derive(Clone, Debug)]
pub struct FileReader {
    size: u64,
    read_bytes: mpsc::UnboundedSender<(Range<u64>, oneshot::Sender<Vec<u8>>)>,
}

impl FileReader {
    pub fn new(file: impl FileSource + 'static) -> Self {
        let (read_tx, mut read_rx) = mpsc::unbounded();
        let reader = Self { size: file.size(), read_bytes: read_tx };

        wasm_bindgen_futures::spawn_local(async move {
            while let Some((range, tx)) = read_rx.next().await {
                let bytes = file.read(range.clone()).await;
                tx.send(bytes).unwrap();
            }
        });

        reader
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub async fn read(&mut self, range: Range<u64>) -> Vec<u8> {
        let (tx, rx) = futures::channel::oneshot::channel::<Vec<u8>>();
        self.read_bytes
            .send((range, tx))
            .await
            .expect("rx was dropped");
        rx.await.expect("failed to read data")
    }
}
