import { defineConfig } from "vite";

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
});
