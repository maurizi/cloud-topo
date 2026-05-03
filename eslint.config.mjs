import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import prettier from "eslint-config-prettier";
import prettierPlugin from "eslint-plugin-prettier";

export default tseslint.config(
  {
    ignores: ["dist/**", "node_modules/**", "*.config.*"]
  },
  {
    files: ["src/**/*.ts"],
    extends: [eslint.configs.recommended, ...tseslint.configs.recommendedTypeChecked, prettier],
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.lint.json"
      }
    },
    plugins: { prettier: prettierPlugin },
    rules: {
      "no-console": "error",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/prefer-promise-reject-errors": "off",
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/consistent-type-imports": [
        "error",
        { prefer: "type-imports", fixStyle: "inline-type-imports" }
      ],
      "prettier/prettier": "error"
    }
  }
);
