use super::ByteTransform;

/// Removes all line break characters (`\n` and `\r`) from the input stream.
///
/// This is a stateless transformer that filters out newline and carriage return
/// characters, effectively joining all lines into a single continuous stream.
#[derive(Clone, Copy, Default, Debug)]
pub struct RemoveLinebreaks;

impl ByteTransform for RemoveLinebreaks {
    type State = ();

    fn transform(&mut self, input: &[u8], _last: bool) -> Vec<u8> {
        // Filter out all newline and carriage return characters
        input.iter().copied().filter(|&b| b != b'\n' && b != b'\r').collect()
    }

    fn transform_len(&mut self, input: &[u8], _last: bool) -> usize {
        // Count bytes that are not newlines or carriage returns
        input.iter().filter(|&&b| b != b'\n' && b != b'\r').count()
    }

    fn state(&self) -> Self::State {}

    fn with_state(&self, _state: &Self::State) -> Self {
        Self
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_linebreaks_basic() {
        let mut transform = RemoveLinebreaks;
        let input = b"hello\nworld\r\n";
        let output = transform.transform(input, false);
        assert_eq!(output, b"helloworld");
    }

    #[test]
    fn test_remove_linebreaks_empty() {
        let mut transform = RemoveLinebreaks;
        let output = transform.transform(b"", false);
        assert_eq!(output, b"");
    }

    #[test]
    fn test_remove_linebreaks_only_linebreaks() {
        let mut transform = RemoveLinebreaks;
        let output = transform.transform(b"\n\r\n\r", false);
        assert_eq!(output, b"");
    }

    #[test]
    fn test_remove_linebreaks_transform_len() {
        let mut transform = RemoveLinebreaks;
        let input = b"hello\nworld\r\n";
        let len = transform.transform_len(input, false);
        assert_eq!(len, 10); // "helloworld"
    }

    #[test]
    fn test_remove_linebreaks_state() {
        let transform = RemoveLinebreaks;
        let state = transform.state();
        let new_transform = transform.with_state(&state);
        assert_eq!(format!("{:?}", transform), format!("{:?}", new_transform));
    }
}
