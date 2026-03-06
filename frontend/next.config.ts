import type { NextConfig } from "next";

const backendBaseUrl =
  process.env.SKU_BACKEND_URL?.trim().replace(/\/+$/, "") || "http://127.0.0.1:5000";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/parse-inventory-api",
        destination: `${backendBaseUrl}/parse-inventory-api`,
      },
      {
        source: "/generate-sku-api",
        destination: `${backendBaseUrl}/generate-sku-api`,
      },
      {
        source: "/analyze-title",
        destination: `${backendBaseUrl}/analyze-title`,
      },
      {
        source: "/admin/:path*",
        destination: `${backendBaseUrl}/admin/:path*`,
      },
      {
        source: "/download/:path*",
        destination: `${backendBaseUrl}/download/:path*`,
      },
    ];
  },
};

export default nextConfig;
