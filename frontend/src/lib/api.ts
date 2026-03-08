import axios from "axios";
import {
    AnalyzeTitleRequest,
    AnalyzeTitleResponse,
    ParseResponse,
    TrainingBootstrapResponse,
    TrainingDatasetUploadResponse,
    TrainingLiveTestResponse,
} from "../types";

// Create an API instance
const apiClient = axios.create({
    // Using empty baseURL so it hits the Next.js server, which then proxies to Flask via next.config.ts
    baseURL: "",
});

type BulkJobQueuedResponse = {
    job_id: string;
    status: "queued" | "running" | "completed" | "failed";
    queue_position?: number;
    poll_interval_ms?: number;
    error?: string;
};

type BulkJobCompletedResponse = BulkJobQueuedResponse & ParseResponse;

const waitFor = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

export const parseInventory = async (
    file: File,
    options?: { onProgress?: (message: string) => void }
): Promise<ParseResponse> => {
    const formData = new FormData();
    formData.append("inventory_file", file);

    const submitResponse = await apiClient.post<BulkJobQueuedResponse>(
        "/parse-inventory-api",
        formData,
    );

    const initial = submitResponse.data;
    const jobId = initial.job_id;
    if (!jobId) {
        throw new Error("Bulk job submission did not return a job id.");
    }

    let pollIntervalMs = Math.max(500, Number(initial.poll_interval_ms || 1000));

    for (;;) {
        const statusResponse = await apiClient.get<BulkJobQueuedResponse | BulkJobCompletedResponse>(
            `/parse-inventory-api/${jobId}`,
        );
        const statusPayload = statusResponse.data;
        const status = statusPayload.status;

        if (status === "completed") {
            return statusPayload as BulkJobCompletedResponse;
        }

        if (status === "failed") {
            throw new Error(statusPayload.error || "Bulk parsing failed.");
        }

        if (status === "queued") {
            const queuePosition = Number(statusPayload.queue_position || 0);
            options?.onProgress?.(
                queuePosition > 0
                    ? `Queued for processing. ${queuePosition} job(s) ahead.`
                    : "Queued for processing.",
            );
        } else {
            options?.onProgress?.("Processing inventory file...");
        }

        pollIntervalMs = Math.max(500, Number(statusPayload.poll_interval_ms || pollIntervalMs || 1000));
        await waitFor(pollIntervalMs);
    }
};

export const analyzeTitle = async (
    payload: AnalyzeTitleRequest,
    options?: { signal?: AbortSignal }
): Promise<AnalyzeTitleResponse> => {
    const response = await apiClient.post<AnalyzeTitleResponse>("/analyze-title", payload, {
        headers: {
            "Content-Type": "application/json",
        },
        signal: options?.signal,
    });
    return response.data;
};

export const getTrainingBootstrap = async (): Promise<TrainingBootstrapResponse> => {
    const response = await apiClient.get<TrainingBootstrapResponse>("/admin/training/bootstrap");
    return response.data;
};

export const addTitleTrainingSample = async (payload: {
    product_title: string;
    detected_model?: string;
    detected_part?: string;
    detected_color?: string;
    expected_sku: string;
}) => {
    const response = await apiClient.post("/admin/training/title-training", payload, {
        headers: { "Content-Type": "application/json" },
    });
    return response.data;
};

export const addSynonymTraining = async (payload: {
    supplier_phrase: string;
    standard_term: string;
}) => {
    const response = await apiClient.post("/admin/training/synonym", payload, {
        headers: { "Content-Type": "application/json" },
    });
    return response.data;
};

export const addSpellingTraining = async (payload: {
    incorrect_word: string;
    correct_word: string;
}) => {
    const response = await apiClient.post("/admin/training/spelling", payload, {
        headers: { "Content-Type": "application/json" },
    });
    return response.data;
};

export const addPartOntologyTraining = async (payload: {
    phrase: string;
    sku_code: string;
}) => {
    const response = await apiClient.post("/admin/training/part-ontology", payload, {
        headers: { "Content-Type": "application/json" },
    });
    return response.data;
};

export const addColorTraining = async (payload: {
    supplier_color: string;
    standard_color: string;
}) => {
    const response = await apiClient.post("/admin/training/color", payload, {
        headers: { "Content-Type": "application/json" },
    });
    return response.data;
};

export const addSkuCorrectionTraining = async (payload: {
    generated_sku: string;
    correct_sku: string;
    title?: string;
}) => {
    const response = await apiClient.post("/admin/training/sku-correction", payload, {
        headers: { "Content-Type": "application/json" },
    });
    return response.data;
};

export const addRuleTraining = async (payload: { rule_text: string }) => {
    const response = await apiClient.post("/admin/training/rule", payload, {
        headers: { "Content-Type": "application/json" },
    });
    return response.data;
};

export const uploadTrainingDataset = async (file: File): Promise<TrainingDatasetUploadResponse> => {
    const formData = new FormData();
    formData.append("file", file);

    const response = await apiClient.post<TrainingDatasetUploadResponse>(
        "/admin/training/upload-dataset",
        formData,
    );
    return response.data;
};

export const runTrainingLiveTest = async (title: string): Promise<TrainingLiveTestResponse> => {
    const response = await apiClient.post<TrainingLiveTestResponse>(
        "/admin/training/live-test",
        { title },
        {
            headers: { "Content-Type": "application/json" },
        },
    );
    return response.data;
};

// Also for downloading the currently processed file if requested later
export const downloadProcessedFileUrl = (filename: string) => {
    return `/download/${encodeURIComponent(filename)}`;
};
