#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from pathlib import Path
from typing import Any

import pandas as pd

from backend.structured_sku_parser import StructuredSKUParserService
from backend.sku_parser import NOT_UNDERSTANDABLE

DEFAULT_CATALOG = Path('/Users/arslan0_0/Downloads/export-all-product-.xlsx')
DEFAULT_BENCHMARK = Path('txparts_benchmark.csv')
DEFAULT_AMBIGUOUS_BENCHMARK = Path('txparts_benchmark_ambiguous.csv')


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == '.csv':
        return pd.read_csv(path)
    return pd.read_excel(path, engine='openpyxl')


def _write_benchmark_rows(output_path: Path, rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=['title', 'expected_sku', 'expected_brand', 'expected_model', 'expected_part'],
        )
        writer.writeheader()
        writer.writerows(rows)


def _normalize_catalog_sku(raw_sku: object) -> str:
    text = str(raw_sku or '').strip().upper()
    text = ' '.join(text.split())
    return text


def build_txparts_benchmark(catalog_path: Path, output_path: Path, *, limit: int | None = None) -> dict[str, Any]:
    df = _read_table(catalog_path)
    if 'Product Name' not in df.columns:
        raise ValueError("Catalog file must include 'Product Name'")
    service = StructuredSKUParserService(enable_ai=False)

    rows: list[dict[str, str]] = []
    seen_titles: set[str] = set()
    duplicate_conflicts = 0
    reviewed_rows = 0

    for idx, raw_row in enumerate(df.fillna('').to_dict(orient='records'), start=1):
        title = str(raw_row.get('Product Name', '') or '').strip()
        if not title:
            continue
        reviewed_rows += 1
        execution = service.analyze_title(title=title, allow_ai=False, mode='single')
        parsed = execution.parsed
        sku = str(parsed.sku or '').strip().upper()
        confidence = float(parsed.final_confidence or parsed.confidence or 0.0)
        part = str(parsed.primary_part or '').strip().upper()
        brand = str(parsed.brand or '').strip().upper()
        model = str(parsed.model or '').strip().upper()
        parse_status = str(execution.parse_status or '').strip().lower()
        reason = str(execution.parser_reason or '').strip().lower()
        parse_stage = str(execution.parse_stage or '').strip().lower()

        if parse_status != 'parsed':
            continue
        if not sku or sku == NOT_UNDERSTANDABLE or len(sku) > 31:
            continue
        if confidence < 0.95:
            continue
        if not brand or not model or not part:
            continue
        if parse_stage != 'rule_only':
            continue
        if any(marker in reason for marker in ('fuzzy', 'vector', 'semantic', 'catalog_title_fuzzy', 'ai')):
            continue
        title_key = title.strip().upper()
        if title_key in seen_titles:
            duplicate_conflicts += 1
            continue
        seen_titles.add(title_key)
        rows.append(
            {
                'title': title,
                'expected_sku': sku,
                'expected_brand': brand,
                'expected_model': model,
                'expected_part': part,
            }
        )
        if limit and len(rows) >= limit:
            break

    _write_benchmark_rows(output_path, rows)

    return {
        'catalog_file': str(catalog_path),
        'benchmark_file': str(output_path),
        'rows_reviewed': reviewed_rows,
        'benchmark_rows': len(rows),
        'duplicate_conflicts_skipped': duplicate_conflicts,
    }


def build_txparts_ambiguous_benchmark(
    catalog_path: Path,
    output_path: Path,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    df = _read_table(catalog_path)
    if 'Product Name' not in df.columns or 'Product Code' not in df.columns:
        raise ValueError("Catalog file must include 'Product Name' and 'Product Code'")
    service = StructuredSKUParserService(enable_ai=False)

    rows: list[dict[str, str]] = []
    seen_titles: set[str] = set()
    reviewed_rows = 0
    skipped_invalid_expected = 0

    for raw_row in df.fillna('').to_dict(orient='records'):
        title = str(raw_row.get('Product Name', '') or '').strip()
        expected_sku = _normalize_catalog_sku(raw_row.get('Product Code', ''))
        if not title or not expected_sku:
            continue
        reviewed_rows += 1
        if expected_sku == NOT_UNDERSTANDABLE or len(expected_sku) > 31:
            skipped_invalid_expected += 1
            continue

        base_execution = service.analyze_title(title=title, allow_ai=False, mode='single')
        is_ambiguous = (
            base_execution.parse_stage != 'rule_only'
            or base_execution.parse_status != 'parsed'
            or float(base_execution.parsed.rule_confidence or 0.0) < 0.80
        )
        if not is_ambiguous:
            continue

        expected_execution = service.analyze_title(
            title=title,
            product_sku=expected_sku,
            allow_ai=False,
            mode='single',
        )
        parsed = expected_execution.parsed
        if expected_execution.parse_status != 'parsed':
            skipped_invalid_expected += 1
            continue
        if parsed.sku != expected_sku:
            skipped_invalid_expected += 1
            continue
        if not parsed.brand or not parsed.model or not parsed.primary_part:
            skipped_invalid_expected += 1
            continue

        title_key = title.strip().upper()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        rows.append(
            {
                'title': title,
                'expected_sku': expected_sku,
                'expected_brand': parsed.brand,
                'expected_model': parsed.model,
                'expected_part': parsed.primary_part,
            }
        )
        if limit and len(rows) >= limit:
            break

    _write_benchmark_rows(output_path, rows)
    return {
        'catalog_file': str(catalog_path),
        'benchmark_file': str(output_path),
        'rows_reviewed': reviewed_rows,
        'benchmark_rows': len(rows),
        'skipped_invalid_expected': skipped_invalid_expected,
    }


def run_benchmark(benchmark_path: Path, *, enable_ai: bool = True) -> dict[str, Any]:
    if not benchmark_path.exists():
        raise FileNotFoundError(f'Benchmark file not found: {benchmark_path}')

    df = pd.read_csv(benchmark_path)
    service = StructuredSKUParserService(enable_ai=enable_ai)

    total = 0
    sku_correct = 0
    brand_correct = 0
    model_correct = 0
    part_correct = 0
    ai_used = 0
    ai_correct = 0
    rule_only_total = 0
    rule_only_correct = 0
    hallucinations = 0
    latencies_ms: list[float] = []

    for row in df.fillna('').to_dict(orient='records'):
        title = str(row.get('title', '') or '').strip()
        if not title:
            continue
        total += 1
        started = time.perf_counter()
        execution = service.analyze_title(title=title, allow_ai=enable_ai, mode='single')
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        latencies_ms.append(elapsed_ms)

        parsed = execution.parsed
        expected_sku = str(row.get('expected_sku', '') or '').strip().upper()
        expected_brand = str(row.get('expected_brand', '') or '').strip().upper()
        expected_model = str(row.get('expected_model', '') or '').strip().upper()
        expected_part = str(row.get('expected_part', '') or '').strip().upper()

        if parsed.sku == expected_sku:
            sku_correct += 1
        if parsed.brand == expected_brand:
            brand_correct += 1
        if parsed.model == expected_model:
            model_correct += 1
        if parsed.primary_part == expected_part:
            part_correct += 1

        if execution.ai_used:
            ai_used += 1
            if parsed.sku == expected_sku:
                ai_correct += 1
            if parsed.primary_part != expected_part:
                hallucinations += 1
        else:
            rule_only_total += 1
            if parsed.sku == expected_sku:
                rule_only_correct += 1

    def pct(value: int, denominator: int) -> float:
        return round((value / denominator), 4) if denominator else 0.0

    if latencies_ms:
        sorted_lat = sorted(latencies_ms)
        p50 = round(statistics.median(sorted_lat), 3)
        p95 = round(sorted_lat[min(len(sorted_lat) - 1, int(len(sorted_lat) * 0.95))], 3)
        p99 = round(sorted_lat[min(len(sorted_lat) - 1, int(len(sorted_lat) * 0.99))], 3)
    else:
        p50 = p95 = p99 = 0.0

    return {
        'benchmark_file': str(benchmark_path),
        'rows_tested': total,
        'sku_accuracy': pct(sku_correct, total),
        'brand_accuracy': pct(brand_correct, total),
        'model_accuracy': pct(model_correct, total),
        'part_accuracy': pct(part_correct, total),
        'hallucination_rate': pct(hallucinations, ai_used),
        'ai_usage_rate': pct(ai_used, total),
        'rule_only_accuracy': pct(rule_only_correct, rule_only_total),
        'ai_assisted_accuracy': pct(ai_correct, ai_used),
        'latency_ms': {
            'p50': p50,
            'p95': p95,
            'p99': p99,
            'max': round(max(latencies_ms), 3) if latencies_ms else 0.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='Build and benchmark the TX Parts SKU parser.')
    parser.add_argument('--catalog', default=str(DEFAULT_CATALOG), help='Path to TX Parts catalog workbook.')
    parser.add_argument('--benchmark', default=str(DEFAULT_BENCHMARK), help='Path to benchmark CSV.')
    parser.add_argument('--ambiguous-benchmark', default=str(DEFAULT_AMBIGUOUS_BENCHMARK), help='Path to ambiguous benchmark CSV.')
    parser.add_argument('--build-only', action='store_true', help='Only build the benchmark CSV.')
    parser.add_argument('--build-ambiguous-only', action='store_true', help='Only build the ambiguous benchmark CSV.')
    parser.add_argument('--limit', type=int, default=0, help='Optional max benchmark rows to build.')
    parser.add_argument('--disable-ai', action='store_true', help='Benchmark in rule-only mode.')
    parser.add_argument('--report-json', default='', help='Optional JSON report output path.')
    args = parser.parse_args()

    catalog_path = Path(args.catalog)
    benchmark_path = Path(args.benchmark)
    ambiguous_benchmark_path = Path(args.ambiguous_benchmark)
    limit = args.limit if args.limit > 0 else None

    if args.build_ambiguous_only:
        build_summary = build_txparts_ambiguous_benchmark(catalog_path, ambiguous_benchmark_path, limit=limit)
        print(json.dumps({'build_summary': build_summary}, indent=2))
        return

    if not benchmark_path.exists() or args.build_only:
        build_summary = build_txparts_benchmark(catalog_path, benchmark_path, limit=limit)
        print(json.dumps({'build_summary': build_summary}, indent=2))
        if args.build_only:
            return

    report = run_benchmark(benchmark_path, enable_ai=not args.disable_ai)
    if args.report_json:
        report_path = Path(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
