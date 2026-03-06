export interface ParsedRow {
    "Product Name": string;
    "Product SKU": string;
    "Product Web SKU": string;
    "Product New SKU": string;
    "SKU Duplicate"?: string;
    "Title Duplicate"?: string;
    [key: string]: unknown;
}

export interface ParseResponse {
    rows: ParsedRow[];
    columns: string[];
    download_file: string;
    stats: {
        total_rows: number;
        parsed_rows: number;
        unparsed_rows: number;
        parse_rate: number;
        sku_duplicates: number;
        title_duplicates: number;
    };
}

export interface AnalyzeTitleRequest {
    title: string;
    product_sku?: string;
    product_web_sku?: string;
    product_description?: string;
}

export interface AnalyzeTitleCorrection {
    from: string;
    to: string;
}

export interface AnalyzeTitleResponse {
    brand: string;
    model: string;
    model_code: string;
    primary_part?: string;
    part: string;
    secondary_part: string | null;
    sku: string;
    confidence: number;
    corrections: string[] | AnalyzeTitleCorrection[];
    correction_pairs?: AnalyzeTitleCorrection[];
    interpreted_title: string;
    parser_reason: string;
    source?: "rule" | "ai" | "cache";
    review_required?: boolean;
    needs_review?: boolean;
    decision?: string;
    parse_status: "parsed" | "not_understandable";
}

export interface TrainingAnalytics {
    total_titles_parsed: number;
    parsing_accuracy: number;
    duplicate_skus: number;
    duplicate_rate: number;
    unknown_parts_detected: number;
}

export interface TrainingTitleSample {
    product_title: string;
    detected_model: string;
    detected_part: string;
    detected_color: string;
    expected_sku: string;
    created_at: string;
}

export interface TrainingPair {
    [key: string]: string;
}

export interface TrainingPartMapping {
    phrase: string;
    sku_code: string;
}

export interface TrainingColorMapping {
    supplier_color: string;
    standard_color: string;
}

export interface TrainingSkuCorrection {
    generated_sku: string;
    correct_sku: string;
}

export interface TrainingRuleDefinition {
    rule_text: string;
    phrase: string;
    sku_code: string;
    created_at: string;
}

export interface TrainingBootstrapMeta {
    synonym_count: number;
    spelling_count: number;
    part_mapping_count: number;
    color_mapping_count: number;
    sku_correction_count: number;
    title_override_count: number;
    rule_count: number;
    training_example_count: number;
    normalization_dataset_loaded: boolean;
}

export interface TrainingBootstrapResponse {
    analytics: TrainingAnalytics;
    title_training_samples: TrainingTitleSample[];
    synonym_mappings: Array<{ supplier_phrase: string; standard_term: string }>;
    spelling_corrections: Array<{ incorrect: string; correct: string }>;
    part_ontology: TrainingPartMapping[];
    color_mappings: TrainingColorMapping[];
    sku_corrections: TrainingSkuCorrection[];
    title_overrides: Array<{ title: string; correct_sku: string }>;
    rule_definitions: TrainingRuleDefinition[];
    learned_pattern_preview: TrainingPartMapping[];
    meta: TrainingBootstrapMeta;
}

export interface TrainingDatasetUploadResponse {
    rows_total: number;
    rows_compared: number;
    matched_rows: number;
    mismatch_rows: number;
    accuracy: number;
    learned_title_overrides: number;
    sample_differences: Array<{
        title: string;
        generated_sku: string;
        correct_sku: string;
    }>;
}

export interface TrainingLiveTestResponse {
    title: string;
    detected_model: string;
    detected_part: string;
    detected_color: string;
    generated_sku: string;
    confidence: number;
    corrections: Array<{ from?: string; to?: string } | string>;
    parse_status: "parsed" | "not_understandable";
    reason: string;
}
