"use client";

import { useState, useEffect, useMemo, useRef } from "react";
import Link from "next/link";
import { FileUploader } from "@/components/FileUploader";
import { ParserControls } from "@/components/ParserControls";
import { ResultsTable } from "@/components/ResultsTable";
import { analyzeTitle, parseInventory, downloadProcessedFileUrl } from "@/lib/api";
import { AnalyzeTitleResponse, ParseResponse } from "@/types";
import { AlertTriangle, CheckCircle2, Box, Loader2, Clipboard, ClipboardCheck, Sparkles, CircleAlert } from "lucide-react";
import axios from "axios";

const PARSER_BACKEND_URL =
  process.env.NEXT_PUBLIC_PARSER_BACKEND_URL?.trim().replace(/\/+$/, "") ||
  "http://127.0.0.1:5000";

function confidenceMeta(confidence: number) {
  if (confidence >= 0.9) {
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

  if (!maybeAxios.response && !maybeAxios.code && typeof maybeAxios.message === "string" && maybeAxios.message.trim()) {
    return maybeAxios.message.trim();
  }

  if (!maybeAxios.response) {
    if (serviceUrl) {
      return `Cannot connect to parser backend at ${serviceUrl}. Start the backend server and retry.`;
    }
    return "Cannot connect to backend service. Start the server and retry.";
  }

  const status = maybeAxios.response.status;
  const apiError = maybeAxios.response.data?.error ?? maybeAxios.response.data?.detail;
  if (typeof apiError === "string" && apiError.trim()) {
    if (status && status >= 500 && /internal server error/i.test(apiError.trim())) {
      if (serviceUrl) {
        return `Cannot connect to parser backend at ${serviceUrl}. Start or restart the backend server and retry.`;
      }
      return "Cannot connect to parser backend. Start or restart the backend server and retry.";
    }
    return apiError;
  }

  if (status && status >= 500) {
    if (serviceUrl) {
      return `Cannot connect to parser backend at ${serviceUrl}. Start or restart the backend server and retry.`;
    }
    return "Cannot connect to parser backend. Start or restart the backend server and retry.";
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
  const [bulkProgressText, setBulkProgressText] = useState<string | null>(null);
  const [data, setData] = useState<ParseResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [singleTitle, setSingleTitle] = useState("");
  const [singleAnalysis, setSingleAnalysis] = useState<AnalyzeTitleResponse | null>(null);
  const [singleAnalysisError, setSingleAnalysisError] = useState<string | null>(null);
  const [isAnalyzingSingle, setIsAnalyzingSingle] = useState(false);
  const [analysisUpdatedAt, setAnalysisUpdatedAt] = useState<number | null>(null);
  const [isSkuCopied, setIsSkuCopied] = useState(false);
  const latestSingleAnalysisRequestRef = useRef(0);
  const singleAnalysisAbortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const title = singleTitle.trim();

    if (!title) {
      latestSingleAnalysisRequestRef.current += 1;
      setSingleAnalysis(null);
      setSingleAnalysisError(null);
      setIsAnalyzingSingle(false);
      setAnalysisUpdatedAt(null);
      return;
    }

    const timeout = window.setTimeout(async () => {
      const requestId = Date.now();
      latestSingleAnalysisRequestRef.current = requestId;
      setSingleAnalysisError(null);
      setIsAnalyzingSingle(true);
      singleAnalysisAbortRef.current?.abort();
      const controller = new AbortController();
      singleAnalysisAbortRef.current = controller;

      try {
        const response = await analyzeTitle({
          title,
        }, { signal: controller.signal });

        if (latestSingleAnalysisRequestRef.current !== requestId) {
          return;
        }

        setSingleAnalysis(response);
        setAnalysisUpdatedAt(Date.now());
        setIsSkuCopied(false);
        if (response.parse_status === "not_understandable") {
          const reason = (response.parser_reason || "").trim();
          if (reason === "display_assembly_filtered") {
            setSingleAnalysisError("Display assembly titles are filtered and not converted to SKU.");
          } else if (reason) {
            setSingleAnalysisError(`Unable to interpret title (${reason}).`);
          } else {
            setSingleAnalysisError("Unable to interpret title.");
          }
        }
      } catch (err) {
        if (axios.isCancel(err)) {
          return;
        }
        if (latestSingleAnalysisRequestRef.current !== requestId) {
          return;
        }
        setSingleAnalysis(null);
        setAnalysisUpdatedAt(null);
        setSingleAnalysisError(
          getApiErrorMessage(
            err,
            "Unable to interpret title",
            PARSER_BACKEND_URL,
          ),
        );
      } finally {
        if (latestSingleAnalysisRequestRef.current === requestId) {
          setIsAnalyzingSingle(false);
        }
      }
    }, 350);

    return () => {
      window.clearTimeout(timeout);
      singleAnalysisAbortRef.current?.abort();
    };
  }, [singleTitle]);

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
    setBulkProgressText("Uploading inventory file...");

    try {
      const response = await parseInventory(file, {
        onProgress: (message) => setBulkProgressText(message),
      });
      setData(response);
      setBulkProgressText(null);
    } catch (err) {
      setBulkProgressText(null);
      setError(
        getApiErrorMessage(
          err,
          "Parser failed. Please check file format or server status.",
          PARSER_BACKEND_URL,
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
    setBulkProgressText(null);
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

  const handleClearSingleSku = () => {
    latestSingleAnalysisRequestRef.current += 1;
    setSingleTitle("");
    setSingleAnalysis(null);
    setSingleAnalysisError(null);
    setIsAnalyzingSingle(false);
    setAnalysisUpdatedAt(null);
    setIsSkuCopied(false);
  };

  const handleCopySku = async () => {
    const sku = singleAnalysis?.parse_status === "parsed" ? singleAnalysis.sku?.trim() : "";
    if (!sku) {
      return;
    }
    try {
      await navigator.clipboard.writeText(sku);
      setIsSkuCopied(true);
      window.setTimeout(() => setIsSkuCopied(false), 1400);
    } catch {
      setIsSkuCopied(false);
    }
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
  const singleConfidenceUi = useMemo(() => {
    if (singleAnalysis?.parse_status === "partial") {
      return {
        label: "Model",
        badgeClass: "bg-blue-100 text-blue-700 border-blue-200",
        barClass: "bg-blue-500",
      };
    }
    return confidenceMeta(singleConfidence);
  }, [singleAnalysis, singleConfidence]);
  const singleSkuDisplay = useMemo(() => {
    if (!singleAnalysis) {
      return "—";
    }
    if (singleAnalysis.parse_status === "partial") {
      return "ADD PART NAME";
    }
    return singleAnalysis.sku?.trim() || "—";
  }, [singleAnalysis]);
  const singleSkuHelpText = useMemo(() => {
    if (!singleAnalysis) {
      return "SKU Generated";
    }
    if (singleAnalysis.parse_status === "partial") {
      return "Model identified. Add a part name to generate the final SKU.";
    }
    return "SKU Generated";
  }, [singleAnalysis]);

  const singleAnalysisStatusMeta = useMemo(() => {
    if (isAnalyzingSingle) {
      return {
        label: "Parsing",
        className: "bg-blue-100 text-blue-700 border-blue-200",
        hint: "Live parser is running",
      };
    }
    if (singleAnalysisError) {
      return {
        label: "Error",
        className: "bg-rose-100 text-rose-700 border-rose-200",
        hint: "Could not parse this title",
      };
    }
    if (!singleAnalysis) {
      return {
        label: "Idle",
        className: "bg-gray-100 text-gray-600 border-gray-200",
        hint: "Start typing to analyze",
      };
    }
    if (singleAnalysis.parse_status === "partial") {
      return {
        label: "Partial",
        className: "bg-blue-100 text-blue-700 border-blue-200",
        hint: "Model detected. Add part name to generate SKU",
      };
    }
    if (singleAnalysis.parse_status === "not_understandable") {
      return {
        label: "Unclear",
        className: "bg-rose-100 text-rose-700 border-rose-200",
        hint: "Needs clearer part/model wording",
      };
    }
    if ((singleAnalysis.confidence ?? 0) >= 0.9) {
      return {
        label: "Ready",
        className: "bg-emerald-100 text-emerald-700 border-emerald-200",
        hint: "High-confidence parse",
      };
    }
    return {
      label: "Review",
      className: "bg-amber-100 text-amber-700 border-amber-200",
      hint: "Verify before final save",
    };
  }, [isAnalyzingSingle, singleAnalysisError, singleAnalysis]);

  const analysisCardTone = useMemo(() => {
    if (singleAnalysisError || singleAnalysis?.parse_status === "not_understandable") {
      return "border-rose-200 bg-white shadow-sm ring-1 ring-rose-50";
    }
    if (singleAnalysis?.parse_status === "partial") {
      return "border-blue-200 bg-white shadow-sm ring-1 ring-blue-50";
    }
    if ((singleAnalysis?.confidence ?? 0) >= 0.9) {
      return "border-emerald-200 bg-white shadow-sm ring-1 ring-emerald-50";
    }
    if ((singleAnalysis?.confidence ?? 0) > 0) {
      return "border-amber-200 bg-white shadow-sm ring-1 ring-amber-50";
    }
    return "border-gray-200 bg-gray-50/50";
  }, [singleAnalysisError, singleAnalysis]);



  const analysisUpdatedLabel = useMemo(() => {
    if (!analysisUpdatedAt) {
      return "";
    }
    return new Date(analysisUpdatedAt).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }, [analysisUpdatedAt]);

  return (
    <div className="min-h-screen bg-[#f8faf9] font-sans text-gray-900 pb-20">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between gap-3">
          <div className="flex items-center gap-3">
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
          <Link
            href="/training"
            className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm font-medium text-gray-700 transition hover:bg-gray-100"
          >
            Training Dashboard
          </Link>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 mt-6 space-y-6">

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
                <div className="flex items-center justify-between">
                  <label className="text-xs font-semibold text-gray-600 uppercase tracking-wide">
                    Product Title
                  </label>
                  <button
                    onClick={handleClearSingleSku}
                    disabled={isAnalyzingSingle}
                    className="text-xs font-medium text-gray-500 hover:text-gray-700 transition-colors disabled:opacity-50"
                  >
                    Clear
                  </button>
                </div>
                <div className="mt-1">
                  <textarea
                    value={singleTitle}
                    onChange={(e) => setSingleTitle(e.target.value)}
                    rows={3}
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                    placeholder="Type a product title..."
                  />
                </div>
              </div>

              <div className={`rounded-xl border p-4 space-y-3 transition-colors ${analysisCardTone}`}>
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h3 className="text-sm font-semibold text-gray-900">Live Analysis</h3>
                    <p className="text-[11px] text-gray-500">
                      Uses the same parser engine and ontology as bulk processing.
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    {analysisUpdatedLabel && (
                      <span className="text-[11px] text-gray-500">Updated {analysisUpdatedLabel}</span>
                    )}
                    <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-semibold ${singleAnalysisStatusMeta.className}`}>
                      {isAnalyzingSingle && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                      {!isAnalyzingSingle && singleAnalysisStatusMeta.label === "Ready" && <Sparkles className="h-3.5 w-3.5" />}
                      {!isAnalyzingSingle && singleAnalysisStatusMeta.label !== "Ready" && singleAnalysisStatusMeta.label !== "Idle" && <CircleAlert className="h-3.5 w-3.5" />}
                      {singleAnalysisStatusMeta.label}
                    </span>
                  </div>
                </div>

                {singleAnalysisError ? (
                  <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                    {singleAnalysisError}
                  </div>
                ) : singleAnalysis ? (
                  <div className="space-y-4">
                    {/* SKU + Confidence row */}
                    <div className="flex items-center justify-between gap-3 rounded-xl border border-gray-200 bg-white shadow-sm px-4 py-3">
                      <div>
                        <p className="text-[10px] font-semibold uppercase tracking-widest text-gray-400 mb-0.5">SKU Generated</p>
                        <p className={`whitespace-pre-wrap font-mono ${singleAnalysis.parse_status === "partial" ? "text-sm font-semibold text-blue-700 tracking-normal" : "text-lg font-bold text-gray-900 tracking-normal"}`}>
                          {singleSkuDisplay}
                        </p>
                        {singleAnalysis.parse_status === "parsed" && singleAnalysis.sku && (() => {
                          const len = (singleAnalysis.sku?.trim() || "").length;
                          const color = len >= 31 ? "text-rose-600 font-bold" : len >= 28 ? "text-amber-600 font-semibold" : "text-gray-400";
                          return (
                            <p className={`mt-0.5 text-[11px] tabular-nums ${color}`}>
                              {len} / 31 characters
                              {len >= 31 && <span className="ml-1 text-rose-500">⚠ over limit</span>}
                            </p>
                          );
                        })()}
                        <p className="mt-1 text-[11px] text-gray-500">{singleSkuHelpText}</p>
                      </div>
                      <div className="flex flex-col items-end gap-2 shrink-0">
                        <div className="flex items-center gap-2">
                          <span className={`text-xs font-semibold px-2 py-0.5 rounded-full border ${singleConfidenceUi.badgeClass}`}>
                            {singleConfidencePercent}% · {singleConfidenceUi.label}
                          </span>
                          <button
                            type="button"
                            onClick={handleCopySku}
                            disabled={singleAnalysis.parse_status !== "parsed" || !singleAnalysis.sku}
                            className="inline-flex items-center justify-center h-7 w-7 rounded-md border border-gray-200 bg-gray-50 text-gray-600 transition-colors hover:bg-gray-100 disabled:cursor-not-allowed disabled:opacity-50"
                            title="Copy SKU"
                          >
                            {isSkuCopied ? <ClipboardCheck className="h-4 w-4 text-emerald-600" /> : <Clipboard className="h-4 w-4" />}
                          </button>
                        </div>
                        <div className="w-24 h-1.5 overflow-hidden rounded-full bg-gray-100">
                          <div className={`h-full rounded-full transition-all ${singleConfidenceUi.barClass}`} style={{ width: `${singleConfidencePercent}%` }} />
                        </div>
                      </div>
                    </div>

                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
                      <div className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
                        <p className="text-[10px] font-semibold uppercase tracking-widest text-gray-400">Brand</p>
                        <p className="mt-1 text-sm font-semibold text-gray-900">{singleAnalysis.brand || "—"}</p>
                      </div>
                      <div className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
                        <p className="text-[10px] font-semibold uppercase tracking-widest text-gray-400">Model</p>
                        <p className="mt-1 text-sm font-semibold text-gray-900">{singleAnalysis.model || "—"}</p>
                      </div>
                      <div className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
                        <p className="text-[10px] font-semibold uppercase tracking-widest text-gray-400">Code</p>
                        <p className="mt-1 text-sm font-semibold text-gray-900">{singleAnalysis.model_code || "—"}</p>
                      </div>
                      <div className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
                        <p className="text-[10px] font-semibold uppercase tracking-widest text-gray-400">Part</p>
                        <p className="mt-1 text-sm font-semibold text-gray-900">{singleAnalysis.part || "—"}</p>
                      </div>
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
                  <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-xs text-gray-500">
                    <p className="font-semibold text-gray-700 mb-1">Start typing to see live analysis.</p>
                    <p>{singleAnalysisStatusMeta.hint}</p>
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
                {(isGenerating || bulkProgressText) && (
                  <p className="mt-3 text-sm text-gray-600">
                    {bulkProgressText || "Processing inventory file..."}
                  </p>
                )}
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
