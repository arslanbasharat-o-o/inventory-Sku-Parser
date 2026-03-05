import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "SKU Parser Engine",
  description: "Mobile Parts Inventory Parser",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body suppressHydrationWarning>{children}</body>
    </html>
  );
}
