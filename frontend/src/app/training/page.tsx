"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  addColorTraining,
  addPartOntologyTraining,
  addRuleTraining,
  addSkuCorrectionTraining,
  addSpellingTraining,
  addSynonymTraining,
  addTitleTrainingSample,
  getTrainingBootstrap,
  runTrainingLiveTest,
  uploadTrainingDataset,
} from "@/lib/api";
import {
  TrainingBootstrapResponse,
  TrainingDatasetUploadResponse,
  TrainingLiveTestResponse,
} from "@/types";
import { Loader2, Upload, FlaskConical, CheckCircle2, AlertTriangle } from "lucide-react";

const TRAINING_BACKEND_URL =
  process.env.NEXT_PUBLIC_PARSER_BACKEND_URL?.trim().replace(/\/+$/, "") ||
  "http://127.0.0.1:5000";

function getApiErrorMessage(err: unknown, fallback: string, serviceUrl = TRAINING_BACKEND_URL): string {
  if (!err || typeof err !== "object") {
    return fallback;
  }

  const maybeAxios = err as {
    response?: {
      data?: { error?: unknown; detail?: unknown } | string;
      status?: number;
    };
  };

  if (!maybeAxios.response) {
    return `Cannot connect to training backend at ${serviceUrl}. Start backend server and retry.`;
  }

  const data = maybeAxios.response.data;
  const status = maybeAxios.response.status ?? 0;

  if (status >= 500) {
    if (typeof data === "string" && data.trim() && !/internal server error/i.test(data.trim())) {
      const shortBody = data.trim();
      if (shortBody.length < 220) {
        return shortBody;
      }
    }
    return `Cannot connect to training backend at ${serviceUrl}. Start backend server and retry.`;
  }

  if (data && typeof data === "object") {
    const apiError = data.error ?? data.detail;
    if (typeof apiError === "string" && apiError.trim()) {
      return apiError;
    }
  }

  if (typeof data === "string" && data.trim()) {
    const body = data.trim();
    if (body.length < 220) {
      return body;
    }
  }

  return fallback;
}

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-2xl border border-gray-200 bg-white p-5 shadow-sm">
      <h2 className="mb-3 text-base font-semibold text-gray-900">{title}</h2>
      <div className="space-y-3">{children}</div>
    </section>
  );
}

function InputField({
  value,
  onChange,
  placeholder,
}: {
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
}) {
  return (
    <input
      value={value}
      onChange={(event) => onChange(event.target.value)}
      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
      placeholder={placeholder}
    />
  );
}

function SubmitButton({
  label,
  loading,
}: {
  label: string;
  loading: boolean;
}) {
  return (
    <button
      type="submit"
      disabled={loading}
      className="inline-flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-emerald-700 disabled:cursor-not-allowed disabled:opacity-60"
    >
      {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <FlaskConical className="h-4 w-4" />}
      {label}
    </button>
  );
}

const EMPTY_TITLE_TRAINING = {
  product_title: "",
  detected_model: "",
  detected_part: "",
  detected_color: "",
  expected_sku: "",
};
const EMPTY_SYNONYM = { supplier_phrase: "", standard_term: "" };
const EMPTY_SPELLING = { incorrect_word: "", correct_word: "" };
const EMPTY_PART = { phrase: "", sku_code: "" };
const EMPTY_COLOR = { supplier_color: "", standard_color: "" };
const EMPTY_SKU_CORRECTION = { generated_sku: "", correct_sku: "", title: "" };

export default function TrainingDashboardPage() {
  const [bootstrap, setBootstrap] = useState<TrainingBootstrapResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const [titleTraining, setTitleTraining] = useState({ ...EMPTY_TITLE_TRAINING });
  const [synonymTraining, setSynonymTraining] = useState({ ...EMPTY_SYNONYM });
  const [spellingTraining, setSpellingTraining] = useState({ ...EMPTY_SPELLING });
  const [partTraining, setPartTraining] = useState({ ...EMPTY_PART });
  const [colorTraining, setColorTraining] = useState({ ...EMPTY_COLOR });
  const [skuCorrection, setSkuCorrection] = useState({ ...EMPTY_SKU_CORRECTION });
  const [ruleText, setRuleText] = useState("");

  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadResult, setUploadResult] = useState<TrainingDatasetUploadResponse | null>(null);

  const [liveTitle, setLiveTitle] = useState("");
  const [liveResult, setLiveResult] = useState<TrainingLiveTestResponse | null>(null);
  const [liveLoading, setLiveLoading] = useState(false);

  // Auto-dismiss success toast after 4s
  useEffect(() => {
    if (!success) return;
    const timer = setTimeout(() => setSuccess(null), 4000);
    return () => clearTimeout(timer);
  }, [success]);

  // Auto-dismiss error toast after 6s
  useEffect(() => {
    if (!error) return;
    const timer = setTimeout(() => setError(null), 6000);
    return () => clearTimeout(timer);
  }, [error]);

  const loadBootstrap = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await getTrainingBootstrap();
      setBootstrap(response);
    } catch (err) {
      setError(getApiErrorMessage(err, "Failed to load training dashboard data."));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadBootstrap();
  }, []);

  const submitAction = async (action: () => Promise<unknown>, successMessage: string, onSuccess?: () => void) => {
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      await action();
      await loadBootstrap();
      setSuccess(successMessage);
      onSuccess?.();
    } catch (err) {
      const message = getApiErrorMessage(err, "Training action failed.");
      setError(message);
    } finally {
      setSaving(false);
    }
  };

  const handleLiveTest = async (event: FormEvent) => {
    event.preventDefault();
    const title = liveTitle.trim();
    if (!title) {
      return;
    }
    setLiveLoading(true);
    setError(null);
    try {
      const result = await runTrainingLiveTest(title);
      setLiveResult(result);
    } catch (err) {
      setError(getApiErrorMessage(err, "Live title test failed."));
      setLiveResult(null);
    } finally {
      setLiveLoading(false);
    }
  };

  const handleUploadDataset = async (event: FormEvent) => {
    event.preventDefault();
    if (!uploadFile) {
      setError("Select a dataset file first.");
      return;
    }
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      const result = await uploadTrainingDataset(uploadFile);
      setUploadResult(result);
      await loadBootstrap();
      setSuccess("Dataset training completed.");
    } catch (err) {
      const message = getApiErrorMessage(err, "Dataset training upload failed.");
      setError(message);
    } finally {
      setSaving(false);
    }
  };

  const analytics = bootstrap?.analytics;
  const summaryCards = useMemo(
    () => [
      {
        label: "Total Titles Parsed",
        value: analytics?.total_titles_parsed ?? 0,
      },
      {
        label: "Parsing Accuracy",
        value: `${(((analytics?.parsing_accuracy ?? 0) * 100) || 0).toFixed(1)}%`,
      },
      {
        label: "Duplicate SKUs",
        value: analytics?.duplicate_skus ?? 0,
      },
      {
        label: "Unknown Parts",
        value: analytics?.unknown_parts_detected ?? 0,
      },
    ],
    [analytics],
  );

  return (
    <div className="min-h-screen bg-[#f7faf8] pb-16">
      <header className="sticky top-0 z-10 border-b border-gray-200 bg-white/95 backdrop-blur">
        <div className="mx-auto flex h-16 max-w-7xl items-center justify-between px-4 sm:px-6 lg:px-8">
          <div>
            <h1 className="text-xl font-bold tracking-tight text-gray-900">SKU Training Dashboard</h1>
            <p className="text-xs uppercase tracking-wide text-gray-500">Admin Controls</p>
          </div>
          <Link
            href="/"
            className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm font-medium text-gray-700 transition hover:bg-gray-100"
          >
            Back to Parser
          </Link>
        </div>
      </header>

      <main className="mx-auto mt-6 max-w-7xl space-y-6 px-4 sm:px-6 lg:px-8">
        {error && (
          <div className="flex items-center gap-2 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 transition-opacity">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            {error}
          </div>
        )}
        {success && (
          <div className="flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700 transition-opacity">
            <CheckCircle2 className="h-4 w-4 shrink-0" />
            {success}
          </div>
        )}

        {loading ? (
          <div className="flex items-center gap-2 rounded-xl border border-gray-200 bg-white px-4 py-4 text-sm text-gray-600">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading dashboard...
          </div>
        ) : (
          <>
            <section className="grid grid-cols-2 gap-4 lg:grid-cols-4">
              {summaryCards.map((card) => (
                <div key={card.label} className="rounded-xl border border-gray-200 bg-white p-4 shadow-sm">
                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">{card.label}</p>
                  <p className="mt-1 text-2xl font-bold text-gray-900">{card.value}</p>
                </div>
              ))}
            </section>

            <section className="grid grid-cols-1 gap-6 xl:grid-cols-2">
              {/* 1. Title Training Panel */}
              <Panel title="1. Title Training Panel">
                <InputField
                  value={titleTraining.product_title}
                  onChange={(value) => setTitleTraining((prev) => ({ ...prev, product_title: value }))}
                  placeholder="Product Title"
                />
                <InputField
                  value={titleTraining.detected_model}
                  onChange={(value) => setTitleTraining((prev) => ({ ...prev, detected_model: value }))}
                  placeholder="Detected Model"
                />
                <InputField
                  value={titleTraining.detected_part}
                  onChange={(value) => setTitleTraining((prev) => ({ ...prev, detected_part: value }))}
                  placeholder="Detected Part"
                />
                <InputField
                  value={titleTraining.detected_color}
                  onChange={(value) => setTitleTraining((prev) => ({ ...prev, detected_color: value }))}
                  placeholder="Detected Color"
                />
                <InputField
                  value={titleTraining.expected_sku}
                  onChange={(value) => setTitleTraining((prev) => ({ ...prev, expected_sku: value }))}
                  placeholder="Expected SKU"
                />
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitAction(
                      () => addTitleTrainingSample(titleTraining),
                      "Title training sample saved.",
                      () => setTitleTraining({ ...EMPTY_TITLE_TRAINING }),
                    );
                  }}
                >
                  <SubmitButton label="Save Training Sample" loading={saving} />
                </form>
                <p className="text-xs text-gray-500">
                  Samples: {bootstrap?.meta.training_example_count ?? 0}
                </p>
                {(bootstrap?.title_training_samples ?? []).length > 0 && (
                  <div className="max-h-36 overflow-auto rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                    {(bootstrap?.title_training_samples ?? []).slice(-5).reverse().map((row, idx) => (
                      <div key={`tt-${idx}`} className="py-0.5 text-gray-700">
                        <span className="font-medium">{row.product_title}</span> → {row.expected_sku}
                      </div>
                    ))}
                  </div>
                )}
              </Panel>

              {/* 2. Synonym Training */}
              <Panel title="2. Synonym Training">
                <InputField
                  value={synonymTraining.supplier_phrase}
                  onChange={(value) => setSynonymTraining((prev) => ({ ...prev, supplier_phrase: value }))}
                  placeholder="Supplier Phrase"
                />
                <InputField
                  value={synonymTraining.standard_term}
                  onChange={(value) => setSynonymTraining((prev) => ({ ...prev, standard_term: value }))}
                  placeholder="Standard Term"
                />
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitAction(
                      () => addSynonymTraining(synonymTraining),
                      "Synonym mapping saved.",
                      () => setSynonymTraining({ ...EMPTY_SYNONYM }),
                    );
                  }}
                >
                  <SubmitButton label="Add Synonym" loading={saving} />
                </form>
                <div className="max-h-36 overflow-auto rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                  {(bootstrap?.synonym_mappings ?? []).slice(0, 10).map((row, idx) => (
                    <div key={`${row.supplier_phrase}-${idx}`} className="py-0.5 text-gray-700">
                      {row.supplier_phrase} → {row.standard_term}
                    </div>
                  ))}
                </div>
              </Panel>

              {/* 3. Spelling Correction Training */}
              <Panel title="3. Spelling Correction Training">
                <InputField
                  value={spellingTraining.incorrect_word}
                  onChange={(value) => setSpellingTraining((prev) => ({ ...prev, incorrect_word: value }))}
                  placeholder="Incorrect Word"
                />
                <InputField
                  value={spellingTraining.correct_word}
                  onChange={(value) => setSpellingTraining((prev) => ({ ...prev, correct_word: value }))}
                  placeholder="Correct Word"
                />
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitAction(
                      () => addSpellingTraining(spellingTraining),
                      "Spelling correction saved.",
                      () => setSpellingTraining({ ...EMPTY_SPELLING }),
                    );
                  }}
                >
                  <SubmitButton label="Add Spelling Rule" loading={saving} />
                </form>
                {(bootstrap?.spelling_corrections ?? []).length > 0 && (
                  <div className="max-h-36 overflow-auto rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                    {(bootstrap?.spelling_corrections ?? []).slice(0, 10).map((row, idx) => (
                      <div key={`sp-${idx}`} className="py-0.5 text-gray-700">
                        {row.incorrect} → {row.correct}
                      </div>
                    ))}
                  </div>
                )}
              </Panel>

              {/* 4. Part Ontology Manager */}
              <Panel title="4. Part Ontology Manager">
                <InputField
                  value={partTraining.phrase}
                  onChange={(value) => setPartTraining((prev) => ({ ...prev, phrase: value }))}
                  placeholder="Phrase"
                />
                <InputField
                  value={partTraining.sku_code}
                  onChange={(value) => setPartTraining((prev) => ({ ...prev, sku_code: value }))}
                  placeholder="SKU Code"
                />
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitAction(
                      () => addPartOntologyTraining(partTraining),
                      "Part ontology mapping saved.",
                      () => setPartTraining({ ...EMPTY_PART }),
                    );
                  }}
                >
                  <SubmitButton label="Save Part Mapping" loading={saving} />
                </form>
                <div className="max-h-40 overflow-auto rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                  {(bootstrap?.part_ontology ?? []).slice(0, 12).map((row) => (
                    <div key={`${row.phrase}-${row.sku_code}`} className="py-0.5 text-gray-700">
                      {row.phrase} → {row.sku_code}
                    </div>
                  ))}
                </div>
              </Panel>

              {/* 5. Color Training */}
              <Panel title="5. Color Training">
                <InputField
                  value={colorTraining.supplier_color}
                  onChange={(value) => setColorTraining((prev) => ({ ...prev, supplier_color: value }))}
                  placeholder="Supplier Color"
                />
                <InputField
                  value={colorTraining.standard_color}
                  onChange={(value) => setColorTraining((prev) => ({ ...prev, standard_color: value }))}
                  placeholder="Standard Color"
                />
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitAction(
                      () => addColorTraining(colorTraining),
                      "Color mapping saved.",
                      () => setColorTraining({ ...EMPTY_COLOR }),
                    );
                  }}
                >
                  <SubmitButton label="Save Color Mapping" loading={saving} />
                </form>
                {(bootstrap?.color_mappings ?? []).length > 0 && (
                  <div className="max-h-36 overflow-auto rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                    {(bootstrap?.color_mappings ?? []).slice(0, 10).map((row, idx) => (
                      <div key={`cl-${idx}`} className="py-0.5 text-gray-700">
                        {row.supplier_color} → {row.standard_color}
                      </div>
                    ))}
                  </div>
                )}
              </Panel>

              {/* 6. SKU Correction Panel */}
              <Panel title="6. SKU Correction Panel">
                <InputField
                  value={skuCorrection.generated_sku}
                  onChange={(value) => setSkuCorrection((prev) => ({ ...prev, generated_sku: value }))}
                  placeholder="Generated SKU"
                />
                <InputField
                  value={skuCorrection.correct_sku}
                  onChange={(value) => setSkuCorrection((prev) => ({ ...prev, correct_sku: value }))}
                  placeholder="Correct SKU"
                />
                <InputField
                  value={skuCorrection.title}
                  onChange={(value) => setSkuCorrection((prev) => ({ ...prev, title: value }))}
                  placeholder="Optional Product Title"
                />
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitAction(
                      () => addSkuCorrectionTraining(skuCorrection),
                      "SKU correction saved.",
                      () => setSkuCorrection({ ...EMPTY_SKU_CORRECTION }),
                    );
                  }}
                >
                  <SubmitButton label="Save SKU Correction" loading={saving} />
                </form>
                {(bootstrap?.sku_corrections ?? []).length > 0 && (
                  <div className="max-h-36 overflow-auto rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                    {(bootstrap?.sku_corrections ?? []).slice(0, 10).map((row, idx) => (
                      <div key={`sc-${idx}`} className="py-0.5 text-gray-700">
                        {row.generated_sku} → {row.correct_sku}
                      </div>
                    ))}
                  </div>
                )}
              </Panel>

              {/* 7. Dataset Training Upload */}
              <Panel title="7. Dataset Training Upload">
                <form onSubmit={handleUploadDataset} className="space-y-3">
                  <label className="flex cursor-pointer items-center gap-2 rounded-lg border border-dashed border-gray-300 bg-gray-50 px-3 py-3 text-sm text-gray-700">
                    <Upload className="h-4 w-4" />
                    <span>{uploadFile ? uploadFile.name : "Upload .xlsx / .xls / .csv"}</span>
                    <input
                      type="file"
                      className="hidden"
                      accept=".xlsx,.xls,.csv"
                      onChange={(event) => {
                        const file = event.target.files?.[0] ?? null;
                        setUploadFile(file);
                      }}
                    />
                  </label>
                  <button
                    type="submit"
                    disabled={saving}
                    className="inline-flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-emerald-700 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
                    Train From Dataset
                  </button>
                </form>
                {uploadResult && (
                  <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-xs text-gray-700">
                    <p>Rows Compared: {uploadResult.rows_compared}</p>
                    <p>Accuracy: {(uploadResult.accuracy * 100).toFixed(1)}%</p>
                    <p>Learned Title Overrides: {uploadResult.learned_title_overrides}</p>
                  </div>
                )}
              </Panel>

              {/* 8. Rule Editor */}
              <Panel title="8. Rule Editor">
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitAction(
                      () => addRuleTraining({ rule_text: ruleText }),
                      "Rule saved to learned patterns.",
                      () => setRuleText(""),
                    );
                  }}
                  className="space-y-3"
                >
                  <textarea
                    value={ruleText}
                    onChange={(event) => setRuleText(event.target.value)}
                    rows={3}
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                    placeholder="If title contains 'back door' then part = BACKDOOR"
                  />
                  <SubmitButton label="Save Rule" loading={saving} />
                </form>
              </Panel>

              {/* 10. Live Title Tester */}
              <Panel title="10. Live Title Tester">
                <form onSubmit={handleLiveTest} className="space-y-3">
                  <textarea
                    value={liveTitle}
                    onChange={(event) => setLiveTitle(event.target.value)}
                    rows={3}
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                    placeholder="pixel 8 pro ear spkeaker"
                  />
                  <button
                    type="submit"
                    disabled={liveLoading}
                    className="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {liveLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <FlaskConical className="h-4 w-4" />}
                    Run Live Test
                  </button>
                </form>
                {liveResult && (
                  <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-xs text-gray-800">
                    <p>Model: {liveResult.detected_model || "—"}</p>
                    <p>Part: {liveResult.detected_part || "—"}</p>
                    <p>Color: {liveResult.detected_color || "—"}</p>
                    <p>SKU: {liveResult.generated_sku || "—"}</p>
                    <p>Confidence: {(liveResult.confidence * 100).toFixed(1)}%</p>
                  </div>
                )}
              </Panel>
            </section>

            {/* 9. Training Analytics Detail */}
            <Panel title="9. Training Analytics Detail">
              <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-5">
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Duplicate Rate: {(((analytics?.duplicate_rate ?? 0) * 100) || 0).toFixed(2)}%
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Synonym Rules: {bootstrap?.meta.synonym_count ?? 0}
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Learned Rules: {bootstrap?.meta.rule_count ?? 0}
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Spelling Rules: {bootstrap?.meta.spelling_count ?? 0}
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Part Mappings: {bootstrap?.meta.part_mapping_count ?? 0}
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Color Mappings: {bootstrap?.meta.color_mapping_count ?? 0}
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  SKU Corrections: {bootstrap?.meta.sku_correction_count ?? 0}
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Title Overrides: {bootstrap?.meta.title_override_count ?? 0}
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-700">
                  Training Samples: {bootstrap?.meta.training_example_count ?? 0}
                </div>
              </div>
            </Panel>
          </>
        )}
      </main>
    </div>
  );
}
