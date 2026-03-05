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

export interface SingleSkuRequest {
    title: string;
    product_sku?: string;
    product_web_sku?: string;
}

export interface SingleSkuResponse {
    title: string;
    product_sku: string;
    product_web_sku: string;
    generated_sku: string;
    parse_status: "parsed" | "not_understandable";
}

export interface AnalyzeTitleRequest {
    title: string;
    product_sku?: string;
    product_web_sku?: string;
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
    parse_status: "parsed" | "not_understandable";
}
