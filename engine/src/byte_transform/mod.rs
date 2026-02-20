pub mod chunked;
pub mod remove_line_breaks;
pub mod utf8_encoder;
pub mod wrap_file;
pub mod wrap_lines;

use std::fmt::Debug;
use std::ops::Range;

/// A trait for byte-level transformations that can be applied to streaming data.
///
/// Implementations can maintain internal state across multiple chunks and must
/// handle both intermediate chunks and the final chunk (indicated by `last`).
pub trait ByteTransform: Sized {
    /// The internal state type that can be saved and restored.
    type State: Clone + Debug;

    /// Transforms a chunk of input bytes, returning the transformed output.
    ///
    /// # Parameters
    /// * `input` - The input bytes to transform
    /// * `last` - Whether this is the final chunk in the stream
    fn transform(&mut self, input: &[u8], last: bool) -> Vec<u8>;

    /// Calculates the output length without producing the actual output.
    /// Updates internal state as if [`transform`](ByteTransform::transform) was called.
    ///
    /// This is more efficient than calling [`transform`](ByteTransform::transform) when
    /// the output will be discarded.
    fn transform_len(&mut self, input: &[u8], last: bool) -> usize {
        self.transform(input, last).len()
    }

    /// Returns a clone of the current internal state.
    fn state(&self) -> Self::State;

    /// Creates a new instance with the given state.
    fn with_state(&self, state: &Self::State) -> Self;
}

pub trait Decoder: Debug {
    fn output_size(&self) -> u64;
    fn calc_input_range(&self, output_range: Range<u64>) -> Range<u64>;
    fn decode_range(&self, src: &[u8], src_offset: u64, dst_range: Range<u64>) -> Vec<u8>;
}

impl<A: ByteTransform, B: ByteTransform> ByteTransform for (A, B) {
    type State = (A::State, B::State);

    fn transform(&mut self, input: &[u8], last: bool) -> Vec<u8> {
        self.1.transform(&self.0.transform(input, last), last)
    }

    fn transform_len(&mut self, input: &[u8], last: bool) -> usize {
        self.1.transform_len(&self.0.transform(input, last), last)
    }

    fn state(&self) -> Self::State {
        (self.0.state(), self.1.state())
    }

    fn with_state(&self, state: &Self::State) -> Self {
        (self.0.with_state(&state.0), self.1.with_state(&state.1))
    }
}
