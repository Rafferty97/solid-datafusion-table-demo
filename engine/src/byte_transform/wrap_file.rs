use super::ByteTransform;

/// Wraps an entire file/stream with a prefix and suffix.
///
/// The prefix is added before the first chunk, and the suffix is added
/// after the last chunk. Unlike [`WrapLines`](WrapLines), this operates on
/// the entire stream rather than individual lines.
#[derive(Clone, Debug)]
pub struct WrapFile {
    prefix: Vec<u8>,
    suffix: Vec<u8>,
    /// Whether we've already emitted the prefix
    started: bool,
}

impl WrapFile {
    /// Creates a new file wrapper with the specified prefix and suffix.
    pub fn new(prefix: impl Into<Vec<u8>>, suffix: impl Into<Vec<u8>>) -> Self {
        Self {
            prefix: prefix.into(),
            suffix: suffix.into(),
            started: false,
        }
    }

    /// Calculates the output length for the given input.
    fn calculate_len(&self, input: &[u8], last: bool) -> usize {
        let mut output_len = input.len();

        if !self.started {
            output_len += self.prefix.len();
        }

        if last {
            output_len += self.suffix.len();
        }

        output_len
    }
}

impl ByteTransform for WrapFile {
    type State = bool;

    fn transform(&mut self, input: &[u8], last: bool) -> Vec<u8> {
        let output_len = self.calculate_len(input, last);
        let mut buffer = Vec::with_capacity(output_len);

        if !self.started {
            buffer.extend_from_slice(&self.prefix);
            self.started = true;
        }

        buffer.extend_from_slice(input);

        if last {
            buffer.extend_from_slice(&self.suffix);
        }

        buffer
    }

    fn transform_len(&mut self, input: &[u8], last: bool) -> usize {
        let output_len = self.calculate_len(input, last);
        self.started = true;
        output_len
    }

    fn state(&self) -> Self::State {
        self.started
    }

    fn with_state(&self, state: &Self::State) -> Self {
        Self {
            prefix: self.prefix.clone(),
            suffix: self.suffix.clone(),
            started: *state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_file_basic() {
        let mut transform = WrapFile::new(b"<start>", b"<end>");
        let output = transform.transform(b"content", true);
        assert_eq!(output, b"<start>content<end>");
    }

    #[test]
    fn test_wrap_file_chunked() {
        let mut transform = WrapFile::new(b"<start>", b"<end>");
        let output1 = transform.transform(b"hello", false);
        let output2 = transform.transform(b" world", true);

        let mut combined = Vec::new();
        combined.extend_from_slice(&output1);
        combined.extend_from_slice(&output2);

        assert_eq!(combined, b"<start>hello world<end>");
    }

    #[test]
    fn test_wrap_file_empty() {
        let mut transform = WrapFile::new(b"<start>", b"<end>");
        let output = transform.transform(b"", true);
        assert_eq!(output, b"<start><end>");
    }

    #[test]
    fn test_wrap_file_state() {
        let mut transform = WrapFile::new(b"<start>", b"<end>");
        transform.transform(b"hello", false);

        let state = transform.state();
        assert!(state); // Should be true (started)

        let mut new_transform = transform.with_state(&state);
        let output = new_transform.transform(b" world", true);
        assert_eq!(output, b" world<end>");
    }

    #[test]
    fn test_wrap_file_transform_len() {
        let mut transform = WrapFile::new(b"<start>", b"<end>");
        let len = transform.transform_len(b"content", true);
        let output = transform.with_state(&false).transform(b"content", true);
        assert_eq!(len, output.len());
    }

    #[test]
    fn test_wrap_file_multiple_chunks_no_prefix_repeat() {
        let mut transform = WrapFile::new(b"<", b">");
        let output1 = transform.transform(b"a", false);
        let output2 = transform.transform(b"b", false);
        let output3 = transform.transform(b"c", true);

        let mut combined = Vec::new();
        combined.extend_from_slice(&output1);
        combined.extend_from_slice(&output2);
        combined.extend_from_slice(&output3);

        assert_eq!(combined, b"<abc>");
    }
}
