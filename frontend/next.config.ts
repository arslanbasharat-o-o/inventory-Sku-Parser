import type { NextConfig } from "next";

const backendBaseUrl =
  process.env.SKU_BACKEND_URL?.trim().replace(/\/+$/, "") || "http://127.0.0.1:5000";
const isCi = process.env.CI === "true";

const nextConfig: NextConfig = {
  experimental: isCi
    ? {
        cpus: 1,
      }
    : undefined,
  typescript: isCi
    ? {
        ignoreBuildErrors: true,
      }
    : undefined,
  async rewrites() {
    return [
      {
        source: "/parse-inventory-api",
        destination: `${backendBaseUrl}/parse-inventory-api`,
      },
      {
        source: "/parse-inventory-api/:path*",
        destination: `${backendBaseUrl}/parse-inventory-api/:path*`,
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
