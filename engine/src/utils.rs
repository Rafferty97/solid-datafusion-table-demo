use wasm_bindgen::prelude::*;

#[cfg(feature = "console_error_panic_hook")]
#[wasm_bindgen(start)]
fn set_panic_hook() {
    console_error_panic_hook::set_once();
}
