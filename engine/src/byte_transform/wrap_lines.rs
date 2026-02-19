use super::ByteTransform;

/// Wraps each line with a prefix and suffix.
///
/// Empty lines (consecutive newlines) are preserved without wrapping.
/// Tracks whether the current position is at the start of a new line.
#[derive(Clone, Debug)]
pub struct WrapLines {
    prefix: Vec<u8>,
    suffix: Vec<u8>,
    /// Whether we're currently at the start of a new line
    empty_line: bool,
}

impl WrapLines {
    /// Creates a new line wrapper with the specified prefix and suffix.
    pub fn new(prefix: impl Into<Vec<u8>>, suffix: impl Into<Vec<u8>>) -> Self {
        Self {
            prefix: prefix.into(),
            suffix: suffix.into(),
            empty_line: true,
        }
    }

    /// Calculates the output length for the given input without modifying state.
    fn calculate_len(&self, input: &[u8], last: bool) -> (usize, bool) {
        if input.is_empty() {
            return (0, self.empty_line);
        }

        // Start with input length, then add prefix/suffix for each non-empty line
        let mut output_len = input.len();

        let affix_len = self.prefix.len() + self.suffix.len();
        output_len += input
            .split(|&c| c == b'\n')
            .map(|line| if line.is_empty() { 0 } else { affix_len })
            .sum::<usize>();

        // Adjust for state transitions at chunk boundaries
        if !self.empty_line {
            let starts_with_newline = input[0] == b'\n';
            match starts_with_newline {
                true => output_len += self.suffix.len(),
                false => output_len -= self.prefix.len(),
            }
        }

        let ends_with_newline = *input.last().unwrap() == b'\n';
        if !last && !ends_with_newline {
            output_len -= self.suffix.len();
        }

        (output_len, ends_with_newline)
    }
}

impl ByteTransform for WrapLines {
    type State = bool;

    fn transform(&mut self, mut input: &[u8], last: bool) -> Vec<u8> {
        let (output_len, _) = self.calculate_len(input, last);
        let mut buffer = Vec::with_capacity(output_len);

        while !input.is_empty() {
            let Some(next_newline) = input.iter().position(|&b| b == b'\n') else {
                // No more newlines - add prefix if at line start, then add remaining input
                if self.empty_line {
                    buffer.extend_from_slice(&self.prefix);
                }
                buffer.extend_from_slice(input);
                self.empty_line = false;
                break;
            };

            match (self.empty_line, next_newline) {
                // Mid-line content followed by newline
                (false, n) => {
                    buffer.extend_from_slice(&input[..n]);
                    buffer.extend_from_slice(&self.suffix);
                    buffer.push(b'\n');
                    self.empty_line = true;
                }
                // Empty line (newline at start of chunk)
                (true, 0) => {
                    buffer.push(b'\n');
                }
                // New line with content
                (true, n) => {
                    buffer.extend_from_slice(&self.prefix);
                    buffer.extend_from_slice(&input[..n]);
                    buffer.extend_from_slice(&self.suffix);
                    buffer.push(b'\n');
                }
            }

            input = &input[(next_newline + 1)..];
            self.empty_line = true;
        }

        // Add suffix if we're at the end and not on an empty line
        if last && !self.empty_line {
            buffer.extend_from_slice(&self.suffix);
        }

        buffer
    }

    fn transform_len(&mut self, input: &[u8], last: bool) -> usize {
        let (output_len, new_state) = self.calculate_len(input, last);
        self.empty_line = new_state;
        output_len
    }

    fn state(&self) -> Self::State {
        self.empty_line
    }

    fn with_state(&self, state: &Self::State) -> Self {
        Self {
            prefix: self.prefix.clone(),
            suffix: self.suffix.clone(),
            empty_line: *state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_lines_single_line() {
        let mut transform = WrapLines::new(b"[", b"]");
        let output = transform.transform(b"hello\n", false);
        assert_eq!(output, b"[hello]\n");
    }

    #[test]
    fn test_wrap_lines_multiple_lines() {
        let mut transform = WrapLines::new(b"[", b"]");
        let output = transform.transform(b"line1\nline2\n", false);
        assert_eq!(output, b"[line1]\n[line2]\n");
    }

    #[test]
    fn test_wrap_lines_empty_line() {
        let mut transform = WrapLines::new(b"[", b"]");
        let output = transform.transform(b"line1\n\nline2\n", false);
        assert_eq!(output, b"[line1]\n\n[line2]\n");
    }

    #[test]
    fn test_wrap_lines_no_trailing_newline() {
        let mut transform = WrapLines::new(b"[", b"]");
        let output = transform.transform(b"hello", false);
        assert_eq!(output, b"[hello");
    }

    #[test]
    fn test_wrap_lines_no_trailing_newline_last() {
        let mut transform = WrapLines::new(b"[", b"]");
        let output = transform.transform(b"hello", true);
        assert_eq!(output, b"[hello]");
    }

    #[test]
    fn test_wrap_lines_chunked() {
        let mut transform = WrapLines::new(b"[", b"]");
        let output1 = transform.transform(b"hel", false);
        let output2 = transform.transform(b"lo\nwor", false);
        let output3 = transform.transform(b"ld\n", true);

        let mut combined = Vec::new();
        combined.extend_from_slice(&output1);
        combined.extend_from_slice(&output2);
        combined.extend_from_slice(&output3);

        assert_eq!(combined, b"[hello]\n[world]\n");
    }

    #[test]
    fn test_wrap_lines_state_management() {
        let mut transform = WrapLines::new(b"[", b"]");
        transform.transform(b"hello", false);

        let state = transform.state();
        assert!(!state); // Should be false (not at line start)

        let mut new_transform = transform.with_state(&state);
        let output = new_transform.transform(b"\n", false);
        assert_eq!(output, b"]\n");
    }

    #[test]
    fn test_wrap_lines_transform_len() {
        let mut transform = WrapLines::new(b"[", b"]");
        let input = b"hello\nworld\n";
        let len = transform.clone().transform_len(input, false);
        let output = transform.transform(input, false);
        println!("{}", String::from_utf8(output.clone()).unwrap());
        assert_eq!(len, output.len());
    }

    #[test]
    fn test_wrap_lines_consecutive_newlines() {
        let mut transform = WrapLines::new(b"[", b"]");
        let output = transform.transform(b"\n\n\n", false);
        assert_eq!(output, b"\n\n\n");
    }

    #[test]
    fn test_wrap_lines_starts_mid_line() {
        let mut transform = WrapLines::new(b"[", b"]");
        transform.empty_line = false; // Simulate mid-line state
        let output = transform.transform(b"world\n", false);
        assert_eq!(output, b"world]\n");
    }
}
