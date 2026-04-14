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
        admin: resolve(__dirname, "admin/index.html"),
        blog: resolve(__dirname, "blog/index.html"),
        "blog-ai-engineer": resolve(__dirname, "blog/ai-engineer-viet-nam/index.html"),
        "blog-backend-engineer": resolve(__dirname, "blog/backend-engineer-viet-nam/index.html"),
        "blog-cam-nang-viec-lam": resolve(__dirname, "blog/cam-nang-viec-lam/index.html"),
        "blog-data-engineer": resolve(__dirname, "blog/data-engineer-viet-nam/index.html"),
        "blog-data-scientist": resolve(__dirname, "blog/data-scientist-viet-nam/index.html"),
        "blog-devops-engineer": resolve(__dirname, "blog/devops-engineer-viet-nam/index.html"),
        "blog-frontend-engineer": resolve(__dirname, "blog/frontend-engineer-viet-nam/index.html"),
        "blog-fullstack-engineer": resolve(__dirname, "blog/fullstack-engineer-viet-nam/index.html"),
        "blog-mobile-developer": resolve(__dirname, "blog/mobile-developer-viet-nam/index.html"),
        "blog-product-manager": resolve(__dirname, "blog/product-manager-viet-nam/index.html"),
        "blog-qa-engineer": resolve(__dirname, "blog/qa-engineer-viet-nam/index.html"),
      },
    },
  },
});
