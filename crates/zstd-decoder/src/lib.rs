// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

// Minimal wasm-bindgen wrapper exposing zstd decompress with a
// prepared dictionary handle. Two API shapes:
//
//   Owning (legacy):
//     new CtopoDecompressor(dict_bytes: Uint8Array)
//     .decompress(src: Uint8Array, capacity: number) -> Uint8Array
//     decompress_no_dict(src: Uint8Array, capacity: number) -> Uint8Array
//   wasm-bindgen materializes the returned Vec<u8> into a fresh JS-
//   heap Uint8Array via `.slice()`. One memcpy per call to leave wasm
//   linear memory.
//
//   In-place (shared-memory):
//     .decompress_into(src, dst_ptr, dst_capacity) -> number
//     decompress_no_dict_into(src, dst_ptr, dst_capacity) -> number
//   Caller pre-allocates `dst_capacity` bytes inside wasm linear
//   memory (via `__wbindgen_export` / malloc), passes the pointer,
//   the decoder writes the output there in place, returns the number
//   of bytes actually written. The caller frees the allocation when
//   it's done with the buffer.
//
//   When the wasm is built with shared memory (+atomics target
//   feature) the linear memory IS a SharedArrayBuffer — Uint8Array
//   views constructed in the main thread over this memory observe
//   the worker's writes without any cross-thread copy.
//
// Built via `npm run build:wasm`; output is checked into
// src/wasm/ so consumers don't need a Rust toolchain.

use wasm_bindgen::prelude::*;
use zstd::bulk::{decompress_to_buffer, Decompressor};

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

    // In-place decompress. `dst_ptr` is a pointer into wasm linear
    // memory pointing at a writable region of size `dst_capacity`.
    // Returns the number of bytes written. Unsafe in the C sense —
    // caller must guarantee the buffer is valid and not aliased by
    // another wasm thread for the duration of the call.
    pub fn decompress_into(
        &mut self,
        src: &[u8],
        dst_ptr: *mut u8,
        dst_capacity: usize,
    ) -> Result<usize, JsValue> {
        // SAFETY: caller asserts dst_ptr..dst_ptr+dst_capacity is a
        // valid writable region in wasm memory not aliased by
        // concurrent threads.
        let dst = unsafe { std::slice::from_raw_parts_mut(dst_ptr, dst_capacity) };
        self.inner
            .decompress_to_buffer(src, dst)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

#[wasm_bindgen]
pub fn decompress_no_dict(src: &[u8], capacity: usize) -> Result<Vec<u8>, JsValue> {
    zstd::bulk::decompress(src, capacity).map_err(|e| JsValue::from_str(&e.to_string()))
}

#[wasm_bindgen]
pub fn decompress_no_dict_into(
    src: &[u8],
    dst_ptr: *mut u8,
    dst_capacity: usize,
) -> Result<usize, JsValue> {
    // SAFETY: caller asserts dst_ptr..dst_ptr+dst_capacity is a
    // valid writable region in wasm memory not aliased by concurrent
    // threads.
    let dst = unsafe { std::slice::from_raw_parts_mut(dst_ptr, dst_capacity) };
    decompress_to_buffer(src, dst).map_err(|e| JsValue::from_str(&e.to_string()))
}
