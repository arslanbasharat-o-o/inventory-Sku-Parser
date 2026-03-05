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
