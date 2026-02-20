use std::ops::Range;

use wasm_bindgen::prelude::*;

#[cfg(feature = "console_error_panic_hook")]
#[wasm_bindgen(start)]
fn set_panic_hook() {
    console_error_panic_hook::set_once();
}

pub fn chunk_ranges(size: u64, chunk_size: usize) -> impl Iterator<Item = (Range<u64>, bool)> {
    (0..size).step_by(chunk_size).map(move |start| {
        let end = (start + chunk_size as u64).min(size);
        (start..end, end == size)
    })
}
