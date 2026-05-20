import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["src/**/__tests__/**/*.test.ts"],
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      // Generated wasm-bindgen glue and tests aren't ours to cover.
      exclude: ["src/**/__tests__/**", "src/core/zstd-wasm/**"],
    },
  },
});
