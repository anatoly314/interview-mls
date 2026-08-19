import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The api serves this build from ./web/dist at /, so everything is same-origin
// in production. In dev, proxy the api routes to the compose-published :8080
// and the app code can keep using relative URLs unchanged.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/jobs": "http://localhost:8080",
      "/healthz": "http://localhost:8080",
      "/ws": { target: "ws://localhost:8080", ws: true },
    },
  },
});
