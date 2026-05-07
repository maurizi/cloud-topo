// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

// Minimal wasm-bindgen wrapper exposing zstd decompress with a
// prepared dictionary handle. Exposes only what cloud-topo's
// section-decompression path needs:
//
//   new CtopoDecompressor(dict_bytes: Uint8Array)
//   .decompress(src: Uint8Array, capacity: number) -> Uint8Array
//   decompress_no_dict(src: Uint8Array, capacity: number) -> Uint8Array
//
// Built via `npm run build:wasm`; output is base64-inlined into
// src/zstd-wasm/zstd-decoder.ts at build time so consumers don't
// need a Rust toolchain.

use wasm_bindgen::prelude::*;
use zstd::bulk::Decompressor;

#[wasm_bindgen]
pub struct CtopoDecompressor {
    inner: Decompressor<'static>,
}

#[wasm_bindgen]
impl CtopoDecompressor {
    #[wasm_bindgen(constructor)]
    pub fn new(dict_bytes: Vec<u8>) -> Result<CtopoDecompressor, JsValue> {
        // Leak the dict bytes so the inner Decompressor can borrow
        // them with 'static lifetime. Dicts are ~110 KiB and live
        // for the module lifetime in our usage; a single leak per
        // CtopoDecompressor is the simplest way to satisfy
        // wasm-bindgen's "no non-'static lifetimes" rule.
        let dict: &'static [u8] = Box::leak(dict_bytes.into_boxed_slice());
        let inner = Decompressor::with_dictionary(dict)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(Self { inner })
    }

    pub fn decompress(&mut self, src: &[u8], capacity: usize) -> Result<Vec<u8>, JsValue> {
        self.inner
            .decompress(src, capacity)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

#[wasm_bindgen]
pub fn decompress_no_dict(src: &[u8], capacity: usize) -> Result<Vec<u8>, JsValue> {
    zstd::bulk::decompress(src, capacity).map_err(|e| JsValue::from_str(&e.to_string()))
}
