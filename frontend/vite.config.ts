import { defineConfig } from "vite";
import { resolve } from "path";

export default defineConfig({
  base: "/",
  server: {
    port: 5173,
    proxy: {
      "/scrape": "http://localhost:8000",
      "/scrape-stream": "http://localhost:8000",
      "/stats": "http://localhost:8000",
    },
  },
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
        privacy: resolve(__dirname, "privacy/index.html"),
        terms: resolve(__dirname, "terms/index.html"),
        contact: resolve(__dirname, "contact/index.html"),
      },
    },
  },
});
