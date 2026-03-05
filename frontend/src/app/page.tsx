"use client";

import { useState, useEffect, useMemo, useRef } from "react";
import { FileUploader } from "@/components/FileUploader";
import { ParserControls } from "@/components/ParserControls";
import { ResultsTable } from "@/components/ResultsTable";
import { analyzeTitle, parseInventory, downloadProcessedFileUrl, generateSingleSku } from "@/lib/api";
import { AnalyzeTitleResponse, ParseResponse } from "@/types";
import { AlertTriangle, CheckCircle2, Box, Loader2 } from "lucide-react";

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
  const [singleTitle, setSingleTitle] = useState("");
  const [singleProductSku, setSingleProductSku] = useState("");
  const [singleWebSku, setSingleWebSku] = useState("");
  const [singleDescription, setSingleDescription] = useState("");
  const [singleSkuResult, setSingleSkuResult] = useState<string | null>(null);
  const [singleStatus, setSingleStatus] = useState<"parsed" | "not_understandable" | null>(null);
  const [singleError, setSingleError] = useState<string | null>(null);
  const [isGeneratingSingle, setIsGeneratingSingle] = useState(false);
  const [singleAnalysis, setSingleAnalysis] = useState<AnalyzeTitleResponse | null>(null);
  const [singleAnalysisError, setSingleAnalysisError] = useState<string | null>(null);
  const [isAnalyzingSingle, setIsAnalyzingSingle] = useState(false);
  const latestSingleAnalysisRequestRef = useRef(0);

  useEffect(() => {
    const title = singleTitle.trim();
    const productSku = singleProductSku.trim();
    const productWebSku = singleWebSku.trim();
    const productDescription = singleDescription.trim();

    if (!title && !productSku && !productWebSku && !productDescription) {
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
          product_description: productDescription,
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
  }, [singleTitle, singleProductSku, singleWebSku, singleDescription]);

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
    if (!singleTitle.trim() && !singleProductSku.trim() && !singleWebSku.trim() && !singleDescription.trim()) {
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
        product_description: singleDescription,
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
    setSingleDescription("");
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

  const singleCorrectionPairs = useMemo(() => {
    if (!singleAnalysis) {
      return [] as { from: string; to: string }[];
    }

    if (singleAnalysis.correction_pairs && singleAnalysis.correction_pairs.length > 0) {
      return singleAnalysis.correction_pairs.filter((item) => item.from || item.to);
    }

    const rawCorrections = Array.isArray(singleAnalysis.corrections)
      ? singleAnalysis.corrections
      : [];

    return rawCorrections
      .map((item) => {
        if (typeof item !== "string") {
          return { from: item.from ?? "", to: item.to ?? "" };
        }

        const arrow = item.includes("->") ? "->" : item.includes("→") ? "→" : "";
        if (!arrow) {
          return { from: item, to: item };
        }

        const [fromToken, toToken] = item.split(arrow, 2);
        const from = (fromToken ?? "").trim();
        const to = (toToken ?? "").trim() || from;
        return { from, to };
      })
      .filter((item) => item.from || item.to);
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

        {/* Success Alert */}
        {data && !error && (
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
                    Product Description (optional)
                  </label>
                  <textarea
                    value={singleDescription}
                    onChange={(e) => setSingleDescription(e.target.value)}
                    rows={2}
                    className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                    placeholder="Extra description text to improve parsing..."
                  />
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
                  <div className="space-y-2.5">
                    {/* SKU + Confidence row */}
                    <div className="flex items-center justify-between gap-3 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
                      <div>
                        <p className="text-[10px] font-semibold uppercase tracking-wide text-gray-400">SKU</p>
                        <p className="text-sm font-bold text-gray-900">{singleAnalysis.sku || "—"}</p>
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        <span className={`text-xs font-semibold px-2 py-0.5 rounded-full border ${singleConfidenceUi.badgeClass}`}>
                          {singleConfidencePercent}% · {singleConfidenceUi.label}
                        </span>
                        <div className="w-16 h-1.5 overflow-hidden rounded-full bg-gray-200">
                          <div className={`h-full rounded-full transition-all ${singleConfidenceUi.barClass}`} style={{ width: `${singleConfidencePercent}%` }} />
                        </div>
                      </div>
                    </div>

                    {/* Details grid */}
                    <div className="grid grid-cols-3 gap-2 text-xs">
                      {[
                        { label: "Brand", value: singleAnalysis.brand },
                        { label: "Model", value: singleAnalysis.model },
                        { label: "Code", value: singleAnalysis.model_code },
                        { label: "Part", value: singleAnalysis.primary_part || singleAnalysis.part },
                        { label: "Sub-part", value: singleAnalysis.secondary_part },
                        { label: "Reason", value: singleAnalysis.parser_reason },
                      ].map(({ label, value }) => (
                        <div key={label} className="rounded-md border border-gray-100 bg-gray-50 px-2 py-1.5">
                          <p className="text-[10px] font-semibold uppercase tracking-wide text-gray-400">{label}</p>
                          <p className="truncate font-semibold text-gray-800">{value || "—"}</p>
                        </div>
                      ))}
                    </div>

                    {/* Spelling corrections — only shown when present */}
                    {singleCorrectionPairs.length > 0 && (
                      <div className="flex flex-wrap gap-1.5 pt-0.5">
                        {singleCorrectionPairs.map((item, idx) => (
                          <span
                            key={idx}
                            className="inline-flex items-center gap-1 rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-xs font-medium text-amber-800"
                          >
                            <span className="font-mono">{item.from}</span>
                            <span className="text-amber-400">→</span>
                            <span className="font-mono">{item.to}</span>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="text-xs text-gray-400">Start typing to see live analysis.</p>
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
