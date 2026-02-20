use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct RecordSet {
    num_rows: usize,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl From<Vec<RecordBatch>> for RecordSet {
    fn from(batches: Vec<RecordBatch>) -> Self {
        let schema = batches
            .get(0)
            .map_or_else(|| Schema::empty().into(), |batch| batch.schema());
        Self::new(schema, batches)
    }
}

impl RecordSet {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let num_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self { num_rows, schema, batches }
    }
}

#[wasm_bindgen]
impl RecordSet {
    pub fn empty() -> Result<Self, String> {
        let schema = Schema::empty().into();
        let batches = vec![RecordBatch::new_empty(schema)];
        Ok(batches.into())
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn encode_schema(&self) -> Vec<u8> {
        use datafusion::arrow::ipc::writer::{
            write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
        };

        let mut buffer = vec![];
        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(true);
        let opts = IpcWriteOptions::default();

        let encoded =
            generator.schema_to_bytes_with_dictionary_tracker(&self.schema, &mut tracker, &opts);
        write_message(&mut buffer, encoded, &opts).unwrap();

        buffer
    }

    pub fn encode_rows(&self, start: usize, end: usize) -> Vec<u8> {
        use datafusion::arrow::ipc::writer::{
            write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
        };

        let mut buffer = vec![];
        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let opts = IpcWriteOptions::default();

        self.batches
            .iter()
            .scan(0, |offset, batch| {
                let output = (*offset, batch);
                *offset += batch.num_rows();
                Some(output)
            })
            .skip_while(|(offset, batch)| start >= offset + batch.num_rows())
            .map_while(|(offset, batch)| {
                let i = start.saturating_sub(offset);
                let j = end.checked_sub(offset)?.min(batch.num_rows());
                Some(batch.slice(i, j - i))
            })
            .for_each(|batch| {
                let (dicts, batch) = generator
                    .encoded_batch(&batch, &mut tracker, &opts)
                    .unwrap();
                assert!(dicts.is_empty());
                write_message(&mut buffer, batch, &opts).unwrap();
            });

        buffer
    }
}
