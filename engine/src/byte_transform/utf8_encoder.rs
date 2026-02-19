use arrayvec::ArrayVec;
use encoding_rs::{CoderResult, DecoderResult, Encoding};

use super::ByteTransform;

/// Converts bytes from a specified encoding to UTF-8.
///
/// Handles partial characters at chunk boundaries by buffering incomplete
/// byte sequences for processing with the next chunk.
#[derive(Clone, Debug)]
pub struct Utf8Encoder {
    encoding: &'static Encoding,
    /// Buffer for incomplete character bytes from the previous chunk
    buffer: ArrayVec<u8, 6>,
}

impl Utf8Encoder {
    /// Creates a new UTF-8 encoder for the specified character encoding.
    pub fn new(encoding: &'static Encoding) -> Self {
        Self { encoding, buffer: ArrayVec::new() }
    }

    /// Decodes input bytes to UTF-8, emitting decoded chunks via the callback.
    /// Returns any incomplete character bytes to buffer for the next chunk.
    fn decode(&self, input: &[u8], last: bool, mut emit: impl FnMut(&[u8])) -> ArrayVec<u8, 6> {
        let mut decoder = self.encoding.new_decoder_without_bom_handling();
        let mut buffer = [0; 2048];
        let mut input = input;

        // Process any buffered bytes from the previous chunk first
        if !self.buffer.is_empty() {
            let (_, read, written, _) = decoder.decode_to_utf8(&self.buffer, &mut buffer, false);
            assert_eq!(read, self.buffer.len());
            emit(&buffer[..written]);
        }

        // Decode the input in chunks until all bytes are consumed
        loop {
            let (state, read, written, _) = decoder.decode_to_utf8(input, &mut buffer, last);

            input = &input[read..];
            emit(&buffer[..written]);

            if state == CoderResult::InputEmpty {
                break;
            }
        }

        if last {
            return ArrayVec::new();
        }

        // Detect incomplete character bytes at the end of this chunk
        let (result, _, _) = decoder.decode_to_utf8_without_replacement(&[], &mut buffer, true);
        match result {
            DecoderResult::InputEmpty => ArrayVec::new(),
            DecoderResult::OutputFull => unreachable!(),
            DecoderResult::Malformed(n, m) => input[(input.len() - (n + m) as usize)..].iter().copied().collect(),
        }
    }
}

impl ByteTransform for Utf8Encoder {
    type State = ArrayVec<u8, 6>;

    fn transform(&mut self, input: &[u8], last: bool) -> Vec<u8> {
        let mut output = Vec::with_capacity(input.len());
        self.buffer = self.decode(input, last, |bytes| output.extend_from_slice(bytes));
        output
    }

    fn transform_len(&mut self, input: &[u8], last: bool) -> usize {
        let mut output_len = 0;
        self.buffer = self.decode(input, last, |bytes| output_len += bytes.len());
        output_len
    }

    fn state(&self) -> Self::State {
        self.buffer.clone()
    }

    fn with_state(&self, state: &Self::State) -> Self {
        let buffer = state.clone();
        Self { buffer, ..*self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_encoder_ascii() {
        let mut encoder = Utf8Encoder::new(encoding_rs::UTF_8);
        let output = encoder.transform(b"hello", true);
        assert_eq!(output, b"hello");
    }

    #[test]
    fn test_utf8_encoder_latin1() {
        let mut encoder = Utf8Encoder::new(encoding_rs::WINDOWS_1252);
        // 0xE9 is 'é' in Windows-1252
        let output = encoder.transform(&[0xE9], true);
        assert_eq!(output, "é".as_bytes());
    }

    #[test]
    fn test_utf8_encoder_chunked() {
        let mut encoder = Utf8Encoder::new(encoding_rs::UTF_8);
        let output1 = encoder.transform(b"hel", false);
        let output2 = encoder.transform(b"lo", true);

        let mut combined = Vec::new();
        combined.extend_from_slice(&output1);
        combined.extend_from_slice(&output2);

        assert_eq!(combined, b"hello");
    }
}
