use super::{ByteTransform, Decoder};
use std::{fmt::Debug, ops::Range};

/// Decoder that uses precomputed boundary mappings
#[derive(Debug)]
pub struct ChunkedDecoder<T: ByteTransform + Clone> {
    transform: T,
    /// Total size of the input file in bytes
    input_size: u64,
    /// Total size of the output file in bytes
    output_size: u64,
    /// Mapping from chunk index to input/output ranges
    chunk_mappings: Vec<ChunkMapping<T>>,
}

/// Mapping entry for a source chunk to its transformed output
#[derive(Debug, Clone)]
struct ChunkMapping<T: ByteTransform + Clone> {
    /// Transformer state at the start of this range
    state: T::State,
    /// Range in the source file
    src_range: Range<u64>,
    /// Range in the output file
    dst_range: Range<u64>,
}

impl<T: ByteTransform + Debug + Clone> Decoder for ChunkedDecoder<T> {
    /// Get the total size of the output file in bytes
    fn output_size(&self) -> u64 {
        self.output_size
    }

    /// Calculate the input range needed to fulfill an output range
    fn calc_input_range(&self, output_range: Range<u64>) -> Range<u64> {
        let start = output_range.start;
        let end = output_range.end.min(self.output_size);

        if end <= start || start >= self.output_size {
            return 0..0;
        }

        let start_chunk = self
            .chunk_mappings
            .iter()
            .find(|c| start >= c.dst_range.start && start < c.dst_range.end)
            .expect("start position not covered by any chunk");

        let end_chunk = self
            .chunk_mappings
            .iter()
            .find(|c| end > c.dst_range.start && end <= c.dst_range.end)
            .expect("end position not covered by any chunk");

        start_chunk.src_range.start..end_chunk.src_range.end
    }

    /// Decode a range of input bytes from the provided input data
    /// Panics if the input data doesn't cover the required source range
    fn decode_range(&self, src: &[u8], src_offset: u64, dst_range: Range<u64>) -> Vec<u8> {
        let start = dst_range.start;
        let end = dst_range.end.min(self.output_size);

        if end <= start || start >= self.output_size {
            return Vec::new();
        }

        // Allocate output buffer
        let mut buffer = Vec::with_capacity((end - start) as usize);

        let chunks = self
            .chunk_mappings
            .iter()
            .skip_while(|c| c.dst_range.end <= start)
            .take_while(|c| c.dst_range.start < end);

        let mut transform: Option<T> = None;

        for chunk in chunks {
            // Initialise the transform if haven't already
            let transform = transform.get_or_insert_with(|| self.transform.with_state(&chunk.state));

            // Extract source data for this chunk
            let source_start = (chunk.src_range.start - src_offset) as usize;
            let source_end = (chunk.src_range.end - src_offset) as usize;
            let source = &src[source_start..source_end];
            let last = chunk.src_range.end >= self.input_size;

            // Decode the entire chunk
            let output = transform.transform(source, last);

            // Append to the output
            let start = dst_range.start.saturating_sub(chunk.dst_range.start) as usize;
            let end = output.len() - (chunk.dst_range.end.saturating_sub(dst_range.end) as usize);
            buffer.extend_from_slice(&output[start..end]);
        }

        buffer
    }
}

/// Builder for creating an encoding transcoder
pub struct ChunkedDecoderBuilder<T: ByteTransform + Clone> {
    transform: T,
    src_pos: usize,
    dst_pos: usize,
    mappings: Vec<ChunkMapping<T>>,
}

impl<T: ByteTransform + Clone + Default> ChunkedDecoderBuilder<T> {
    /// Create a new builder for the given encoding
    #[allow(unused)]
    pub fn new() -> Self {
        Self::new_with_state(Default::default())
    }
}

impl<T: ByteTransform + Clone> ChunkedDecoderBuilder<T> {
    pub fn new_with_state(transform: T) -> Self {
        Self {
            transform,
            src_pos: 0,
            dst_pos: 0,
            mappings: Vec::new(),
        }
    }

    /// Feed a sequential chunk of encoded data to build the mapping
    pub fn feed(&mut self, chunk: &[u8], last: bool) {
        let state = self.transform.state();
        let src_start = self.src_pos as u64;
        let dst_start = self.dst_pos as u64;

        self.src_pos += chunk.len();
        self.dst_pos += self.transform.transform_len(chunk, last);

        let src_end = self.src_pos as u64;
        let dst_end = self.dst_pos as u64;

        // Push the new chunk mapping
        self.mappings.push(ChunkMapping {
            state,
            src_range: (src_start..src_end),
            dst_range: (dst_start..dst_end),
        });
    }

    /// Finalize and build the transcoder
    pub fn build(self) -> ChunkedDecoder<T> {
        ChunkedDecoder {
            transform: self.transform,
            input_size: self.src_pos as u64,
            output_size: self.dst_pos as u64,
            chunk_mappings: self.mappings,
        }
    }
}
