import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/parse-inventory-api",
        destination: "http://127.0.0.1:5000/parse-inventory-api",
      },
      {
        source: "/generate-sku-api",
        destination: "http://127.0.0.1:5000/generate-sku-api",
      },
      {
        source: "/analyze-title",
        destination: "http://127.0.0.1:5000/analyze-title",
      },
      {
        source: "/admin/:path*",
        destination: "http://127.0.0.1:5000/admin/:path*",
      },
      {
        source: "/download/:path*",
        destination: "http://127.0.0.1:5000/download/:path*",
      },
    ];
  },
};

export default nextConfig;
