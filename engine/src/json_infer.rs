use wasm_bindgen::prelude::*;

/// The detected kind of a JSON input stream.
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    /// The file is a sequence of top-level JSON objects (newline-delimited JSON / JSON Lines).
    JsonLines,
    /// The entire file is a single top-level JSON array.
    JsonArray,
    /// The file contains one or more top-level non-object JSON values (strings, numbers,
    /// booleans, null, or arrays that are *not* the only value).
    JsonValues,
}

/// Errors that can be returned by the detector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectorError {
    /// An unexpected byte was encountered (e.g. a second top-level value whose type is
    /// incompatible with the first).
    Incompatible(String),
}

impl std::fmt::Display for DetectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetectorError::Incompatible(msg) => write!(f, "incompatible JSON values: {msg}"),
        }
    }
}

impl std::error::Error for DetectorError {}

// ---------------------------------------------------------------------------
// Internal state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Haven't seen any non-whitespace byte yet.
    Initial,
    /// We are inside a top-level JSON string (first value).
    InString { escaped: bool },
    /// We are inside the first top-level array, tracking bracket depth.
    InFirstArray {
        depth: usize,
        in_string: bool,
        escaped: bool,
    },
    /// First value was an array; we finished it and are now scanning for a second value.
    AfterFirstArray,
    /// We determined the kind; just drain any remaining bytes.
    Done(JsonKind),
}

/// Streaming JSON kind detector.
///
/// Feed chunks of bytes via [`JsonDetector::feed`].  The method returns
/// `Ok(Some(kind))` once the kind is known, `Ok(None)` if more data is
/// needed, or `Err(e)` on an incompatibility error.
pub struct JsonDetector {
    phase: Phase,
}

impl JsonDetector {
    pub fn new() -> Self {
        Self { phase: Phase::Initial }
    }

    /// Feed a chunk of bytes.  Returns:
    /// - `Ok(false)` – need more data; keep feeding
    /// - `Ok(true)`  – kind determined; call [`finish`](Self::finish) to retrieve it
    /// - `Err(e)`    – incompatible values detected
    pub fn feed(&mut self, chunk: &[u8]) -> Result<bool, DetectorError> {
        for &b in chunk {
            self.step(b)?;
            if matches!(self.phase, Phase::Done(_)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Signal end-of-stream.  Resolves any ambiguity that required seeing a
    /// second value (e.g. a lone top-level array → `JsonArray`).
    pub fn finish(self) -> Result<JsonKind, DetectorError> {
        match self.phase {
            Phase::Done(k) => Ok(k),
            Phase::AfterFirstArray => {
                // Only one top-level value and it was an array → JsonArray.
                Ok(JsonKind::JsonArray)
            }
            Phase::Initial => {
                // Empty / whitespace-only input: treat as JsonLines (no values).
                Ok(JsonKind::JsonLines)
            }
            Phase::InString { .. } => Ok(JsonKind::JsonValues),
            Phase::InFirstArray { .. } => Ok(JsonKind::JsonArray),
        }
    }

    fn step(&mut self, b: u8) -> Result<(), DetectorError> {
        match &mut self.phase {
            // ---------------------------------------------------------------
            Phase::Initial => {
                if b.is_ascii_whitespace() {
                    // skip
                } else if b == b'{' {
                    // First value is an object → JsonLines immediately.
                    self.phase = Phase::Done(JsonKind::JsonLines);
                } else if b == b'[' {
                    // First value is an array.  Track it to see if there is a
                    // second top-level value afterwards.
                    self.phase = Phase::InFirstArray {
                        depth: 1,
                        in_string: false,
                        escaped: false,
                    };
                } else if b == b'"' {
                    // First value is a string.
                    self.phase = Phase::InString { escaped: false };
                } else {
                    // number / true / false / null → JsonValues immediately.
                    self.phase = Phase::Done(JsonKind::JsonValues);
                }
            }

            // ---------------------------------------------------------------
            Phase::InString { escaped } => {
                if *escaped {
                    *escaped = false;
                } else if b == b'\\' {
                    *escaped = true;
                } else if b == b'"' {
                    // String value finished → JsonValues.
                    self.phase = Phase::Done(JsonKind::JsonValues);
                }
            }

            // ---------------------------------------------------------------
            Phase::InFirstArray { depth, in_string, escaped } => {
                if *in_string {
                    if *escaped {
                        *escaped = false;
                    } else if b == b'\\' {
                        *escaped = true;
                    } else if b == b'"' {
                        *in_string = false;
                    }
                } else {
                    match b {
                        b'"' => *in_string = true,
                        b'[' => *depth += 1,
                        b']' => {
                            *depth -= 1;
                            if *depth == 0 {
                                // Finished the first array.
                                self.phase = Phase::AfterFirstArray;
                            }
                        }
                        _ => {}
                    }
                }
            }

            // ---------------------------------------------------------------
            // After we have seen one complete top-level array, look for a
            // second top-level value.
            Phase::AfterFirstArray => {
                if b.is_ascii_whitespace() {
                    // skip
                } else if b == b'['
                    || b == b'"'
                    || b.is_ascii_digit()
                    || b == b'-'
                    || b == b't'
                    || b == b'f'
                    || b == b'n'
                {
                    // A second non-object value: array + scalar → JsonValues.
                    self.phase = Phase::Done(JsonKind::JsonValues);
                } else if b == b'{' {
                    // array followed by object: incompatible.  The detector is
                    // dead after returning Err, so there is no need to store
                    // the error in self.phase.
                    return Err(DetectorError::Incompatible(
                        "top-level array followed by object".into(),
                    ));
                }
                // Any other byte (e.g. stray comma) is ignored; real JSON
                // parsers would reject it but we are only detecting kind.
            }

            // ---------------------------------------------------------------
            Phase::Done(_) => {
                // Nothing to do; caller drains remaining bytes.
            }
        }
        Ok(())
    }
}

impl Default for JsonDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn detect(input: &[u8]) -> Result<JsonKind, DetectorError> {
        let mut d = JsonDetector::new();
        if d.feed(input)? {
            return d.finish();
        }
        d.finish()
    }

    fn detect_chunked(chunks: &[&[u8]]) -> Result<JsonKind, DetectorError> {
        let mut d = JsonDetector::new();
        for chunk in chunks {
            if d.feed(chunk)? {
                return d.finish();
            }
        }
        d.finish()
    }

    // -- JsonLines -----------------------------------------------------------

    #[test]
    fn single_object_is_jsonlines() {
        assert_eq!(detect(b"{\"a\":1}"), Ok(JsonKind::JsonLines));
    }

    #[test]
    fn multiple_objects_is_jsonlines() {
        assert_eq!(detect(b"{\"a\":1}\n{\"b\":2}"), Ok(JsonKind::JsonLines));
    }

    #[test]
    fn object_with_array_field_is_jsonlines() {
        assert_eq!(detect(b"{\"arr\":[1,2,3]}"), Ok(JsonKind::JsonLines));
    }

    // -- JsonArray -----------------------------------------------------------

    #[test]
    fn lone_array_is_jsonarray() {
        assert_eq!(detect(b"[1,2,3]"), Ok(JsonKind::JsonArray));
    }

    #[test]
    fn lone_array_with_trailing_whitespace() {
        assert_eq!(detect(b"[1,2,3]   \n"), Ok(JsonKind::JsonArray));
    }

    #[test]
    fn nested_arrays_lone_is_jsonarray() {
        assert_eq!(detect(b"[[1,[2,3]],4]"), Ok(JsonKind::JsonArray));
    }

    #[test]
    fn array_containing_brackets_in_strings() {
        // The inner "]" and "[" are inside a string and must not confuse depth tracking.
        assert_eq!(detect(br#"["hello [world]", 42]"#), Ok(JsonKind::JsonArray));
    }

    // -- JsonValues ----------------------------------------------------------

    #[test]
    fn two_arrays_is_jsonvalues() {
        assert_eq!(detect(b"[1,2]\n[3,4]"), Ok(JsonKind::JsonValues));
    }

    #[test]
    fn number_is_jsonvalues() {
        assert_eq!(detect(b"42"), Ok(JsonKind::JsonValues));
    }

    #[test]
    fn string_is_jsonvalues() {
        assert_eq!(detect(br#""hello""#), Ok(JsonKind::JsonValues));
    }

    #[test]
    fn string_with_escaped_bracket_is_jsonvalues() {
        // Make sure escaped quotes inside strings don't confuse us.
        assert_eq!(
            detect(br#""he said \"hi\" [to me]""#),
            Ok(JsonKind::JsonValues)
        );
    }

    #[test]
    fn boolean_is_jsonvalues() {
        assert_eq!(detect(b"true"), Ok(JsonKind::JsonValues));
    }

    #[test]
    fn null_is_jsonvalues() {
        assert_eq!(detect(b"null"), Ok(JsonKind::JsonValues));
    }

    // -- Errors --------------------------------------------------------------

    #[test]
    fn array_then_object_is_error() {
        assert!(detect(b"[1,2]\n{\"a\":1}").is_err());
    }

    // -- Chunked delivery ----------------------------------------------------

    #[test]
    fn lone_array_chunked() {
        assert_eq!(
            detect_chunked(&[b"[1", b",2", b",3]"]),
            Ok(JsonKind::JsonArray)
        );
    }

    #[test]
    fn two_arrays_chunked() {
        assert_eq!(
            detect_chunked(&[b"[1,2]", b"\n[3,4]"]),
            Ok(JsonKind::JsonValues)
        );
    }

    #[test]
    fn string_with_bracket_chunked() {
        assert_eq!(
            detect_chunked(&[br#"["a [b"#, br#"] c"]"#]),
            Ok(JsonKind::JsonArray)
        );
    }
}
