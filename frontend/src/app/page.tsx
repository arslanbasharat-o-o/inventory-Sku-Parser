"use client";

import { useState, useEffect, useMemo, useRef } from "react";
import { FileUploader } from "@/components/FileUploader";
import { ParserControls } from "@/components/ParserControls";
import { ResultsTable } from "@/components/ResultsTable";
import { analyzeTitle, parseInventory, downloadProcessedFileUrl, generateSingleSku } from "@/lib/api";
import { AnalyzeTitleResponse, ParseResponse } from "@/types";
import { AlertTriangle, CheckCircle2, Box, Loader2, Sparkles } from "lucide-react";

const STORAGE_KEY = "sku_parser_last_result";
const EXAMPLE_TITLES = [
  "Samsung A52 Charging Port",
  "Galaxy A50 Battery",
  "Pixel 7 Pro Back Camera",
  "Samsung A30 Power Volume Flex",
  "Googel Pixle 9A Battry",
];

function confidenceMeta(confidence: number) {
  if (confidence > 0.9) {
    return {
      label: "High",
      badgeClass: "bg-emerald-100 text-emerald-700 border-emerald-200",
      barClass: "bg-emerald-500",
    };
  }
  if (confidence >= 0.7) {
    return {
      label: "Review",
      badgeClass: "bg-amber-100 text-amber-700 border-amber-200",
      barClass: "bg-amber-500",
    };
  }
  return {
    label: "Low",
    badgeClass: "bg-rose-100 text-rose-700 border-rose-200",
    barClass: "bg-rose-500",
  };
}

function getApiErrorMessage(
  err: unknown,
  fallback: string,
  serviceUrl?: string,
): string {
  if (!err || typeof err !== "object") {
    return fallback;
  }

  const maybeAxios = err as {
    code?: string;
    message?: string;
    response?: {
      status?: number;
      data?: { error?: unknown; detail?: unknown };
    };
  };

  if (!maybeAxios.response) {
    if (serviceUrl) {
      return `Cannot connect to parser backend at ${serviceUrl}. Start the backend server and retry.`;
    }
    return "Cannot connect to backend service. Start the server and retry.";
  }

  const status = maybeAxios.response.status;
  const apiError = maybeAxios.response.data?.error ?? maybeAxios.response.data?.detail;
  if (typeof apiError === "string" && apiError.trim()) {
    return apiError;
  }

  if (status === 413) {
    return "File is too large. Maximum upload size is 20 MB.";
  }
  if (status === 429) {
    return "Server is busy processing other files. Please retry in a moment.";
  }

  return fallback;
}

export default function Dashboard() {
  const [file, setFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [isGenerating, setIsGenerating] = useState(false);
  const [data, setData] = useState<ParseResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [restoredFromCache, setRestoredFromCache] = useState(false);
  const [singleTitle, setSingleTitle] = useState("");
  const [singleProductSku, setSingleProductSku] = useState("");
  const [singleWebSku, setSingleWebSku] = useState("");
  const [singleSkuResult, setSingleSkuResult] = useState<string | null>(null);
  const [singleStatus, setSingleStatus] = useState<"parsed" | "not_understandable" | null>(null);
  const [singleError, setSingleError] = useState<string | null>(null);
  const [isGeneratingSingle, setIsGeneratingSingle] = useState(false);
  const [singleAnalysis, setSingleAnalysis] = useState<AnalyzeTitleResponse | null>(null);
  const [singleAnalysisError, setSingleAnalysisError] = useState<string | null>(null);
  const [isAnalyzingSingle, setIsAnalyzingSingle] = useState(false);
  const latestSingleAnalysisRequestRef = useRef(0);

  // Restore from localStorage on mount
  useEffect(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed: ParseResponse = JSON.parse(saved);
        setData(parsed);
        setRestoredFromCache(true);
      }
    } catch {
      // ignore corrupt data
    }
  }, []);

  useEffect(() => {
    const title = singleTitle.trim();
    const productSku = singleProductSku.trim();
    const productWebSku = singleWebSku.trim();

    if (!title && !productSku && !productWebSku) {
      latestSingleAnalysisRequestRef.current += 1;
      setSingleAnalysis(null);
      setSingleAnalysisError(null);
      setIsAnalyzingSingle(false);
      return;
    }

    const timeout = window.setTimeout(async () => {
      const requestId = Date.now();
      latestSingleAnalysisRequestRef.current = requestId;
      setSingleAnalysisError(null);
      setIsAnalyzingSingle(true);

      try {
        const response = await analyzeTitle({
          title,
          product_sku: productSku,
          product_web_sku: productWebSku,
        });

        if (latestSingleAnalysisRequestRef.current !== requestId) {
          return;
        }

        setSingleAnalysis(response);
        if (response.parse_status === "not_understandable") {
          setSingleAnalysisError("Unable to interpret title");
        }
      } catch (err) {
        if (latestSingleAnalysisRequestRef.current !== requestId) {
          return;
        }
        console.error(err);
        setSingleAnalysis(null);
        setSingleAnalysisError(
          getApiErrorMessage(
            err,
            "Unable to interpret title",
            "http://127.0.0.1:8000",
          ),
        );
      } finally {
        if (latestSingleAnalysisRequestRef.current === requestId) {
          setIsAnalyzingSingle(false);
        }
      }
    }, 350);

    return () => window.clearTimeout(timeout);
  }, [singleTitle, singleProductSku, singleWebSku]);

  // Drag and drop handlers
  const handleDragEnter = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);

    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      const droppedFile = e.dataTransfer.files[0];
      if (
        droppedFile.name.endsWith(".xlsx") ||
        droppedFile.name.endsWith(".csv") ||
        droppedFile.name.endsWith(".xls") ||
        droppedFile.name.endsWith(".xlsm")
      ) {
        setFile(droppedFile);
        setError(null);
      } else {
        setError("Invalid file format. Please upload .xlsx or .csv");
      }
    }
  };

  const handleFileSelect = (selectedFile: File) => {
    setFile(selectedFile);
    setError(null);
  };

  const handleGenerate = async () => {
    if (!file) {
      setError("Please select a file first.");
      return;
    }

    setIsGenerating(true);
    setError(null);

    try {
      const response = await parseInventory(file);
      setData(response);
      setRestoredFromCache(false);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(response));
    } catch (err) {
      console.error(err);
      setError(
        getApiErrorMessage(
          err,
          "Parser failed. Please check file format or server status.",
          "http://127.0.0.1:5000",
        ),
      );
    } finally {
      setIsGenerating(false);
    }
  };

  const handleClear = () => {
    setData(null);
    setFile(null);
    setError(null);
    setRestoredFromCache(false);
    localStorage.removeItem(STORAGE_KEY);
  };

  const handleDownload = () => {
    if (!data || !data.download_file) return;
    const link = document.createElement("a");
    link.href = downloadProcessedFileUrl(data.download_file);
    link.download = data.download_file;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleGenerateSingleSku = async () => {
    if (!singleTitle.trim() && !singleProductSku.trim() && !singleWebSku.trim()) {
      setSingleError("Enter a title or SKU hint first.");
      return;
    }

    setIsGeneratingSingle(true);
    setSingleError(null);

    try {
      const response = await generateSingleSku({
        title: singleTitle,
        product_sku: singleProductSku,
        product_web_sku: singleWebSku,
      });
      setSingleSkuResult(response.generated_sku);
      setSingleStatus(response.parse_status);
    } catch (err) {
      console.error(err);
      setSingleError(
        getApiErrorMessage(
          err,
          "Failed to generate SKU. Check server status.",
          "http://127.0.0.1:5000",
        ),
      );
      setSingleSkuResult(null);
      setSingleStatus(null);
    } finally {
      setIsGeneratingSingle(false);
    }
  };

  const handleClearSingleSku = () => {
    latestSingleAnalysisRequestRef.current += 1;
    setSingleTitle("");
    setSingleProductSku("");
    setSingleWebSku("");
    setSingleSkuResult(null);
    setSingleStatus(null);
    setSingleError(null);
    setSingleAnalysis(null);
    setSingleAnalysisError(null);
    setIsAnalyzingSingle(false);
  };

  const singleConfidencePercent = useMemo(() => {
    const value = singleAnalysis?.confidence ?? 0;
    return Math.max(0, Math.min(100, Math.round(value * 100)));
  }, [singleAnalysis]);

  const singleConfidence = singleAnalysis?.confidence ?? 0;
  const singleConfidenceUi = confidenceMeta(singleConfidence);

  return (
    <div className="min-h-screen bg-[#f8faf9] font-sans text-gray-900 pb-20">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-[1600px] mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-emerald-600 flex items-center justify-center text-white shadow-sm">
            <Box size={18} />
          </div>
          <div>
            <h1 className="text-xl font-bold font-sans tracking-tight text-gray-900 leading-tight">
              SKU Parser Engine
            </h1>
            <p className="text-xs text-gray-500 font-medium tracking-wide uppercase">
              Mobile Parts Inventory Parser
            </p>
          </div>
        </div>
      </header>

      <main className="max-w-[1600px] mx-auto px-4 sm:px-6 lg:px-8 mt-6 space-y-6">

        {/* Error Alert */}
        {error && (
          <div
            style={{ animation: "fadeSlideIn 300ms cubic-bezier(0.22, 1, 0.36, 1) both" }}
            className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-xl flex items-center gap-3 shadow-sm"
          >
            <AlertTriangle size={20} className="text-red-500 flex-shrink-0" />
            <p className="text-sm font-medium">{error}</p>
          </div>
        )}

        {/* Restored-from-cache banner */}
        {restoredFromCache && !error && (
          <div className="bg-amber-50 border border-amber-300 text-amber-900 px-4 py-3 rounded-xl flex items-center gap-3 shadow-sm">
            <AlertTriangle size={18} className="text-amber-500 flex-shrink-0" />
            <p className="text-sm font-medium flex-1">
              ⚠️ Showing <strong>old cached results</strong> — duplicate counts may be incorrect. Upload your file and click <strong>Generate SKUs</strong> to get fresh accurate results.
            </p>
            <button
              onClick={handleClear}
              className="text-xs font-semibold bg-amber-200 hover:bg-amber-300 text-amber-900 px-3 py-1.5 rounded-lg transition-colors whitespace-nowrap"
            >
              Clear Cache
            </button>
          </div>
        )}

        {/* Success Alert */}
        {data && !error && !restoredFromCache && (
          <div
            style={{ animation: "fadeSlideIn 300ms cubic-bezier(0.22, 1, 0.36, 1) both" }}
            className="bg-emerald-50 border border-emerald-200 text-emerald-800 px-4 py-3 rounded-xl flex items-center gap-3 shadow-sm"
          >
            <CheckCircle2 size={20} className="text-emerald-500 flex-shrink-0" />
            <p className="text-sm font-medium">
              Successfully parsed {data.stats?.parsed_rows} out of {data.stats?.total_rows} rows.
            </p>
          </div>
        )}

        <div className="flex flex-col gap-6 items-stretch">
          {/* Top Row: Two Equal Width Cards */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-stretch">
            {/* Left Card: Single Generator */}
            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 flex flex-col gap-4 h-full">
              <div>
                <h2 className="text-lg font-semibold text-gray-800">Single Title SKU Generator</h2>
                <p className="text-sm text-gray-500">
                  Enter one product title and get the SKU instantly.
                </p>
              </div>

              <div className="space-y-3">
                <div>
                  <label className="text-xs font-semibold text-gray-600 uppercase tracking-wide">
                    Product Title
                  </label>
                  <textarea
                    value={singleTitle}
                    onChange={(e) => setSingleTitle(e.target.value)}
                    rows={3}
                    className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                    placeholder="Type a product title..."
                  />
                </div>

                <div className="grid grid-cols-1 gap-3">
                  <div>
                    <label className="text-xs font-semibold text-gray-600 uppercase tracking-wide">
                      Product SKU (optional)
                    </label>
                    <input
                      value={singleProductSku}
                      onChange={(e) => setSingleProductSku(e.target.value)}
                      className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                      placeholder="A525PBF"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-semibold text-gray-600 uppercase tracking-wide">
                      Product Web SKU (optional)
                    </label>
                    <input
                      value={singleWebSku}
                      onChange={(e) => setSingleWebSku(e.target.value)}
                      className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                      placeholder="WEB-A525-PBF"
                    />
                  </div>
                </div>

                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
                  <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-600">
                    Examples
                  </p>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                    {EXAMPLE_TITLES.map((example) => (
                      <button
                        key={example}
                        type="button"
                        onClick={() => setSingleTitle(example)}
                        className="rounded-lg border border-gray-200 bg-white px-3 py-2 text-left text-xs font-medium text-gray-700 transition hover:border-emerald-300 hover:text-gray-900"
                      >
                        {example}
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              {singleError && (
                <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
                  {singleError}
                </div>
              )}

              {singleSkuResult && (
                <div
                  className={`rounded-lg border px-3 py-2 ${singleStatus === "parsed"
                    ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                    : "border-amber-200 bg-amber-50 text-amber-800"
                    }`}
                >
                  <p className="text-xs font-semibold uppercase tracking-wide">Generated SKU (Manual Run)</p>
                  <p className="text-base font-bold">{singleSkuResult}</p>
                </div>
              )}

              <div className="flex flex-col sm:flex-row gap-2">
                <button
                  onClick={handleGenerateSingleSku}
                  disabled={isGeneratingSingle}
                  className="w-full sm:w-auto flex items-center justify-center gap-2 bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2.5 rounded-lg font-medium transition-colors disabled:opacity-70 disabled:cursor-not-allowed"
                >
                  {isGeneratingSingle ? "Generating..." : "Generate SKU"}
                </button>
                <button
                  onClick={handleClearSingleSku}
                  disabled={isGeneratingSingle}
                  className="w-full sm:w-auto flex items-center justify-center gap-2 bg-white hover:bg-gray-50 text-gray-700 border border-gray-200 px-4 py-2.5 rounded-lg font-medium transition-colors disabled:opacity-50"
                >
                  Clear
                </button>
              </div>

              <div className="rounded-xl border border-gray-200 p-4 space-y-3">
                <div className="flex items-center justify-between gap-3">
                  <h3 className="text-sm font-semibold text-gray-900">Live Analysis</h3>
                  {isAnalyzingSingle && (
                    <div className="inline-flex items-center gap-1.5 rounded-full border border-gray-200 bg-gray-50 px-2.5 py-1 text-xs font-semibold text-gray-600">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      Parsing
                    </div>
                  )}
                </div>

                {singleAnalysisError ? (
                  <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                    {singleAnalysisError}
                  </div>
                ) : singleAnalysis ? (
                  <div className="space-y-3">
                    <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
                      <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">Predicted SKU</p>
                      <p className="mt-1 text-base font-bold text-gray-900">{singleAnalysis.sku || "-"}</p>
                    </div>

                    <div className="rounded-lg border border-gray-200 p-3">
                      <div className="mb-2 flex items-center justify-between">
                        <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">Confidence</p>
                        <span className={`inline-flex items-center rounded-full border px-2 py-1 text-xs font-semibold ${singleConfidenceUi.badgeClass}`}>
                          {singleConfidenceUi.label}
                        </span>
                      </div>
                      <div className="mb-1.5 h-2 overflow-hidden rounded-full bg-gray-100">
                        <div
                          className={`h-full transition-all ${singleConfidenceUi.barClass}`}
                          style={{ width: `${singleConfidencePercent}%` }}
                        />
                      </div>
                      <p className="text-sm font-semibold text-gray-700">{singleConfidencePercent}%</p>
                    </div>

                    <div className="rounded-lg border border-gray-200 p-3">
                      <h4 className="text-xs font-semibold uppercase tracking-wide text-gray-500">Parsing Details</h4>
                      <dl className="mt-2 grid grid-cols-1 gap-2 text-sm sm:grid-cols-2">
                        <div>
                          <dt className="text-gray-500">Brand detected</dt>
                          <dd className="font-semibold text-gray-800">{singleAnalysis.brand || "-"}</dd>
                        </div>
                        <div>
                          <dt className="text-gray-500">Model detected</dt>
                          <dd className="font-semibold text-gray-800">{singleAnalysis.model || "-"}</dd>
                        </div>
                        <div>
                          <dt className="text-gray-500">Model code</dt>
                          <dd className="font-semibold text-gray-800">{singleAnalysis.model_code || "-"}</dd>
                        </div>
                        <div>
                          <dt className="text-gray-500">Primary component</dt>
                          <dd className="font-semibold text-gray-800">{singleAnalysis.part || "-"}</dd>
                        </div>
                        <div>
                          <dt className="text-gray-500">Secondary component</dt>
                          <dd className="font-semibold text-gray-800">{singleAnalysis.secondary_part || "-"}</dd>
                        </div>
                        <div>
                          <dt className="text-gray-500">Parser reason</dt>
                          <dd className="font-semibold text-gray-800">{singleAnalysis.parser_reason || "-"}</dd>
                        </div>
                      </dl>
                    </div>

                    <div className="rounded-lg border border-gray-200 p-3">
                      <div className="mb-2 flex items-center gap-2">
                        <Sparkles className="h-4 w-4 text-gray-500" />
                        <h4 className="text-xs font-semibold uppercase tracking-wide text-gray-500">Spelling Corrections</h4>
                      </div>
                      {singleAnalysis.corrections.length > 0 ? (
                        <ul className="space-y-2">
                          {singleAnalysis.corrections.map((item, idx) => (
                            <li
                              key={`${item.from}-${item.to}-${idx}`}
                              className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-xs font-medium text-gray-700"
                            >
                              <span className="font-mono text-gray-900">{item.from}</span>
                              <span className="px-2 text-gray-400">→</span>
                              <span className="font-mono text-gray-900">{item.to}</span>
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <p className="text-sm text-gray-600">No spelling corrections were applied.</p>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-600">
                    Start typing in the title input to see live analysis.
                  </div>
                )}
              </div>
            </div>

            {/* Right Card: Bulk Upload & Controls */}
            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 flex flex-col gap-6 h-full">
              <div>
                <h2 className="text-lg font-semibold text-gray-800">Bulk Inventory Parser</h2>
                <p className="text-sm text-gray-500">Upload your file, generate SKUs, and review the parsed inventory.</p>
              </div>

              <div className="flex-1">
                <FileUploader
                  file={file}
                  onFileSelect={handleFileSelect}
                  isDragging={isDragging}
                  onDragEnter={handleDragEnter}
                  onDragLeave={handleDragLeave}
                  onDragOver={handleDragOver}
                  onDrop={handleDrop}
                />
              </div>

              <div className="pt-4 border-t border-gray-100 mt-auto">
                <ParserControls
                  onGenerate={handleGenerate}
                  onClear={handleClear}
                  onDownload={handleDownload}
                  isGenerating={isGenerating}
                  hasData={!!data}
                />
              </div>
            </div>
          </div>

          {/* Stats Row */}
          {data && (
            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
                <div className="bg-gray-50 rounded-lg p-3 border border-gray-100">
                  <p className="text-xs text-gray-500 uppercase font-semibold">Total Rows</p>
                  <p className="text-xl font-bold text-gray-900">{data.stats?.total_rows}</p>
                </div>
                <div className="bg-emerald-50 rounded-lg p-3 border border-emerald-100">
                  <p className="text-xs text-emerald-600/80 uppercase font-semibold">Parsed</p>
                  <p className="text-xl font-bold text-emerald-700">{data.stats?.parsed_rows}</p>
                </div>
                <div className="bg-blue-50 rounded-lg p-3 border border-blue-100">
                  <p className="text-xs text-blue-600/80 uppercase font-semibold">Accuracy</p>
                  <p className="text-xl font-bold text-blue-700">
                    {data.stats?.total_rows
                      ? `${((data.stats.parsed_rows / data.stats.total_rows) * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
                <div className="bg-red-50 rounded-lg p-3 border border-red-100">
                  <p className="text-xs text-red-600/80 uppercase font-semibold">Dup SKUs</p>
                  <p className="text-xl font-bold text-red-700">{data.stats?.sku_duplicates}</p>
                </div>
                <div className="bg-amber-50 rounded-lg p-3 border border-amber-100">
                  <p className="text-xs text-amber-600/80 uppercase font-semibold">Dup Titles</p>
                  <p className="text-xl font-bold text-amber-700">{data.stats?.title_duplicates}</p>
                </div>
              </div>
            </div>
          )}

          {/* Skeleton while parsing */}
          {isGenerating && (
            <div className="bg-white border rounded-xl shadow-sm overflow-hidden">
              {/* Toolbar skeleton */}
              <div className="p-4 border-b flex gap-4">
                <div className="h-9 w-56 bg-gray-100 rounded-lg animate-pulse" />
                <div className="ml-auto h-9 w-36 bg-gray-100 rounded-lg animate-pulse" />
                <div className="h-9 w-24 bg-gray-100 rounded-lg animate-pulse" />
              </div>
              {/* Header skeleton */}
              <div className="flex gap-4 px-4 py-3 border-b bg-gray-50">
                {["40%", "15%", "15%", "15%", "7%", "7%"].map((w, i) => (
                  <div key={i} className="h-3 bg-gray-200 rounded animate-pulse" style={{ width: w }} />
                ))}
              </div>
              {/* Row skeletons */}
              {Array.from({ length: 12 }).map((_, i) => (
                <div
                  key={i}
                  className="flex gap-4 px-4 py-3 border-b"
                  style={{ opacity: 1 - i * 0.06 }}
                >
                  {["40%", "15%", "15%", "15%", "7%", "7%"].map((w, j) => (
                    <div
                      key={j}
                      className="h-3 bg-gray-100 rounded animate-pulse"
                      style={{ width: w, animationDelay: `${j * 40}ms` }}
                    />
                  ))}
                </div>
              ))}
            </div>
          )}

          {/* Full width table — fades and slides in */}
          {data && !isGenerating && (
            <div
              style={{
                animation: "fadeSlideIn 400ms cubic-bezier(0.22, 1, 0.36, 1) both",
              }}
            >
              <ResultsTable data={data.rows} />
            </div>
          )}

          <style>{`
          @keyframes fadeSlideIn {
            from { opacity: 0; transform: translateY(16px); }
            to   { opacity: 1; transform: translateY(0); }
          }
        `}</style>
        </div>
      </main>
    </div>
  );
}
