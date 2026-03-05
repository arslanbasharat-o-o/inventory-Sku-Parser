#!/usr/bin/env python3
"""Comprehensive validation framework for the SKU Parser Engine.

This runner validates parser behavior across clean titles, typo tolerance,
priority rules, learning subsystems, filtering rules, confidence bands,
and large-dataset performance.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from sku_intelligence_engine import EngineConfig, NOT_UNDERSTANDABLE, SKUIntelligenceEngine


@dataclass
class ValidationCase:
    case_id: str
    title: str
    expected_sku: str | None = None
    expected_parse_status: str = "parsed"
    expected_primary_part: str | None = None
    expected_secondary_part: str | None = None
    expected_interpreted_tokens: tuple[str, ...] = ()
    product_sku: str = ""
    product_web_sku: str = ""


@dataclass
class ValidationResult:
    category: str
    case_id: str
    title: str
    passed: bool
    expected: str
    actual: str
    confidence: float | None = None
    parser_reason: str = ""
    details: str = ""


@dataclass
class ValidationSummary:
    total_tests_run: int
    tests_passed: int
    tests_failed: int
    pass_rate: float
    average_confidence_score: float | None
    new_patterns_learned: int
    new_spelling_variations_learned: int


@dataclass
class FrameworkConfig:
    include_performance: bool = True
    enable_vector_layer: bool = False
    pattern_min_frequency: int = 5
    spelling_promotion_threshold: int = 3
    performance_sizes: tuple[int, ...] = (1000, 10000, 100000)
    performance_threshold_seconds: dict[int, float] = field(
        default_factory=lambda: {
            1000: 5.0,
            10000: 45.0,
            100000: 180.0,
        }
    )


class SKUValidationFramework:
    """Runs full validation categories and emits structured reports."""

    def __init__(
        self,
        base_dir: Path,
        workspace_dir: Path,
        config: FrameworkConfig | None = None,
        reset_workspace: bool = True,
    ) -> None:
        self.base_dir = base_dir.resolve()
        self.workspace_dir = workspace_dir.resolve()
        self.config = config or FrameworkConfig()
        self.reset_workspace = reset_workspace

        self.category_results: dict[str, list[ValidationResult]] = {}
        self._confidence_samples: list[float] = []
        self.new_patterns: dict[str, str] = {}
        self.new_spelling_variations: dict[str, str] = {}

        self._prepare_workspace()
        self.engine = self._build_engine()

    @property
    def learned_patterns_file(self) -> Path:
        return self.workspace_dir / "learned_patterns.json"

    @property
    def learned_spelling_file(self) -> Path:
        return self.workspace_dir / "learned_spelling_variations.json"

    @property
    def training_patterns_file(self) -> Path:
        return self.workspace_dir / "training_patterns.json"

    def _prepare_workspace(self) -> None:
        if self.reset_workspace and self.workspace_dir.exists():
            shutil.rmtree(self.workspace_dir)
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

        required_copy_files = [
            "mobile_parts_ontology.json",
            "mobile_parts_dictionary.json",
            "part_code_rules.json",
            "spelling_corrections.json",
        ]
        for filename in required_copy_files:
            src = self.base_dir / filename
            if not src.exists():
                raise FileNotFoundError(f"Required file not found: {src}")
            shutil.copy2(src, self.workspace_dir / filename)

        self._write_json(self.workspace_dir / "learned_patterns.json", {})
        self._write_json(self.workspace_dir / "learned_title_patterns.json", {})
        self._write_json(self.workspace_dir / "learned_parts.json", {})
        self._write_json(self.workspace_dir / "learned_spelling_variations.json", {})
        self._write_json(self.workspace_dir / "unknown_parts_log.json", [])
        self._write_json(self.workspace_dir / "training_patterns.json", {})

    def _build_engine(self) -> SKUIntelligenceEngine:
        cfg = EngineConfig(
            ontology_file=self.workspace_dir / "mobile_parts_ontology.json",
            dictionary_file=self.workspace_dir / "mobile_parts_dictionary.json",
            part_rules_file=self.workspace_dir / "part_code_rules.json",
            learned_patterns_file=self.workspace_dir / "learned_patterns.json",
            legacy_learned_title_patterns_file=self.workspace_dir / "learned_title_patterns.json",
            legacy_learned_parts_file=self.workspace_dir / "learned_parts.json",
            unknown_log_file=self.workspace_dir / "unknown_parts_log.json",
            training_patterns_file=self.workspace_dir / "training_patterns.json",
            spelling_corrections_file=self.workspace_dir / "spelling_corrections.json",
            learned_spelling_variations_file=self.workspace_dir / "learned_spelling_variations.json",
            pattern_min_frequency=self.config.pattern_min_frequency,
            spelling_promotion_threshold=self.config.spelling_promotion_threshold,
            enable_vector_layer=self.config.enable_vector_layer,
        )
        return SKUIntelligenceEngine(config=cfg)

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    @staticmethod
    def _read_json(path: Path, fallback: Any) -> Any:
        if not path.exists():
            return fallback
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return fallback

    def _record(
        self,
        category: str,
        case_id: str,
        title: str,
        passed: bool,
        expected: str,
        actual: str,
        confidence: float | None = None,
        parser_reason: str = "",
        details: str = "",
    ) -> None:
        row = ValidationResult(
            category=category,
            case_id=case_id,
            title=title,
            passed=passed,
            expected=expected,
            actual=actual,
            confidence=confidence,
            parser_reason=parser_reason,
            details=details,
        )
        self.category_results.setdefault(category, []).append(row)
        if confidence is not None:
            self._confidence_samples.append(float(confidence))

    def _run_case(self, category: str, case: ValidationCase) -> None:
        payload = self.engine.analyze_title(
            case.title,
            product_sku=case.product_sku,
            product_web_sku=case.product_web_sku,
        )
        confidence = float(payload.get("confidence", 0.0) or 0.0)
        parser_reason = str(payload.get("reason", ""))

        checks: list[bool] = []
        expected_parts: list[str] = []
        actual_parts: list[str] = []
        details: list[str] = []

        actual_sku = str(payload.get("sku", ""))
        actual_status = str(payload.get("parse_status", ""))
        actual_part = str(payload.get("part", ""))
        actual_secondary = str(payload.get("secondary_part", ""))
        actual_interpreted = str(payload.get("interpreted_title", "")).lower()

        if case.expected_sku is not None:
            expected_parts.append(f"sku={case.expected_sku}")
            actual_parts.append(f"sku={actual_sku}")
            checks.append(actual_sku == case.expected_sku)

        if case.expected_parse_status:
            expected_parts.append(f"parse_status={case.expected_parse_status}")
            actual_parts.append(f"parse_status={actual_status}")
            checks.append(actual_status == case.expected_parse_status)

        if case.expected_primary_part is not None:
            expected_parts.append(f"part={case.expected_primary_part}")
            actual_parts.append(f"part={actual_part}")
            checks.append(actual_part == case.expected_primary_part)

        if case.expected_secondary_part is not None:
            expected_parts.append(f"secondary_part={case.expected_secondary_part}")
            actual_parts.append(f"secondary_part={actual_secondary}")
            checks.append(actual_secondary == case.expected_secondary_part)

        if case.expected_interpreted_tokens:
            missing = [
                token
                for token in case.expected_interpreted_tokens
                if token.lower() not in actual_interpreted
            ]
            expected_parts.append(
                "interpreted_contains=" + ",".join(case.expected_interpreted_tokens)
            )
            actual_parts.append(f"interpreted={actual_interpreted}")
            checks.append(not missing)
            if missing:
                details.append("missing_tokens=" + ",".join(missing))

        passed = all(checks) if checks else True
        self._record(
            category=category,
            case_id=case.case_id,
            title=case.title,
            passed=passed,
            expected="; ".join(expected_parts) if expected_parts else "",
            actual="; ".join(actual_parts) if actual_parts else "",
            confidence=confidence,
            parser_reason=parser_reason,
            details="; ".join(details),
        )

    def _category_1_clean_titles(self) -> None:
        category = "CATEGORY 1 — CLEAN TITLES"
        cases = [
            ValidationCase(
                case_id="clean_battery",
                title="Samsung Galaxy A52 A525 Battery",
                expected_sku="GALAXY A52 A525 BATT",
                expected_primary_part="BATT",
            ),
            ValidationCase(
                case_id="clean_charging_port",
                title="Samsung Galaxy A71 A716 Charging Port",
                expected_sku="GALAXY A71 A716 CP",
                expected_primary_part="CP",
            ),
            ValidationCase(
                case_id="clean_power_volume_flex",
                title="Samsung Galaxy A30 A305 Power Volume Flex",
                expected_sku="GALAXY A30 A305 PV-F",
                expected_primary_part="PV-F",
            ),
        ]
        for case in cases:
            self._run_case(category, case)

    def _category_2_spelling_mistakes(self) -> None:
        category = "CATEGORY 2 — SPELLING MISTAKES"
        cases = [
            ValidationCase(
                case_id="typo_battery",
                title="Samsng A52 Battry",
                expected_sku="GALAXY A52 BATT",
                expected_primary_part="BATT",
                expected_interpreted_tokens=("samsung", "a52", "battery"),
            ),
            ValidationCase(
                case_id="typo_ear_speaker",
                title="Galaxi A71 Ear Speker",
                expected_sku="GALAXY A71 ES",
                expected_primary_part="ES",
                expected_interpreted_tokens=("galaxy", "a71", "ear", "speaker"),
            ),
            ValidationCase(
                case_id="typo_camera",
                title="Pixle 9A Camra",
                expected_sku="PIXEL 9A BC",
                expected_primary_part="BC",
                expected_interpreted_tokens=("pixel", "9a", "camera"),
            ),
            ValidationCase(
                case_id="typo_power_volume_flex",
                title="Samung A30 Powr Volum Flex",
                expected_sku="GALAXY A30 PV-F",
                expected_primary_part="PV-F",
                expected_interpreted_tokens=("samsung", "a30", "power", "volume", "flex"),
            ),
        ]
        for case in cases:
            self._run_case(category, case)

    def _category_3_multi_component_titles(self) -> None:
        category = "CATEGORY 3 — MULTI-COMPONENT TITLES"
        cases = [
            ValidationCase(
                case_id="multi_cp_hj",
                title="Galaxy A71 A716 Charging Port With Headphone Jack",
                expected_sku="GALAXY A71 A716 CP HJ",
                expected_primary_part="CP",
                expected_secondary_part="HJ",
            ),
            ValidationCase(
                case_id="multi_es_ps",
                title="Galaxy A50 A505 Ear Speaker Proximity Sensor",
                expected_sku="GALAXY A50 A505 ES-PS",
                expected_primary_part="ES-PS",
                expected_secondary_part="",
            ),
            ValidationCase(
                case_id="multi_nfc_cf",
                title="Galaxy A52 A525 Wireless NFC Charging Flex",
                expected_sku="GALAXY A52 A525 NFC CF",
                expected_primary_part="NFC",
                expected_secondary_part="CF",
            ),
        ]
        for case in cases:
            self._run_case(category, case)

    def _category_4_flex_components(self) -> None:
        category = "CATEGORY 4 — FLEX COMPONENTS"
        cases = [
            ValidationCase(
                case_id="flex_power",
                title="Galaxy A52 A525 Power Flex",
                expected_sku="GALAXY A52 A525 P-F",
                expected_primary_part="P-F",
            ),
            ValidationCase(
                case_id="flex_volume",
                title="Galaxy A52 A525 Volume Flex",
                expected_sku="GALAXY A52 A525 VOL-F",
                expected_primary_part="VOL-F",
            ),
            ValidationCase(
                case_id="flex_power_volume",
                title="Galaxy A52 A525 Power Volume Flex",
                expected_sku="GALAXY A52 A525 PV-F",
                expected_primary_part="PV-F",
            ),
        ]
        for case in cases:
            self._run_case(category, case)

    def _category_5_dataset_training_validation(self) -> None:
        category = "CATEGORY 5 — DATASET TRAINING VALIDATION"
        baseline_patterns = self._read_json(self.learned_patterns_file, {})

        rows: list[dict[str, str]] = []
        for _ in range(6):
            rows.append(
                {
                    "Product Name": "Galaxy A52 A525 Charging Socket Board",
                    "Product SKU": "",
                    "Product Web SKU": "",
                }
            )
            rows.append(
                {
                    "Product Name": "Galaxy A52 A525 Vibration Motor",
                    "Product SKU": "",
                    "Product Web SKU": "",
                }
            )
            rows.append(
                {
                    "Product Name": "Galaxy A52 A525 Ear Receiver",
                    "Product SKU": "",
                    "Product Web SKU": "",
                }
            )

        train_input = self.workspace_dir / "category5_training_input.xlsx"
        train_output = self.workspace_dir / "category5_training_output.xlsx"
        train_review = self.workspace_dir / "category5_review_queue.xlsx"
        pd.DataFrame(rows).to_excel(train_input, index=False)
        self.engine.process_inventory(train_input, train_output, train_review)

        learned_patterns = self._read_json(self.learned_patterns_file, {})
        expected_map = {
            "charging socket": "CP",
            "vibration motor": "VIB",
            "ear receiver": "ES",
        }

        for phrase, expected_code in expected_map.items():
            actual_code = str(learned_patterns.get(phrase, ""))
            self._record(
                category=category,
                case_id=f"learned_pattern_{phrase.replace(' ', '_')}",
                title=phrase,
                passed=actual_code == expected_code,
                expected=expected_code,
                actual=actual_code,
                details="learned_patterns.json mapping check",
            )

        self.new_patterns = {
            key: value
            for key, value in learned_patterns.items()
            if baseline_patterns.get(key) != value
        }
        self._record(
            category=category,
            case_id="new_patterns_count",
            title="learned_patterns.json",
            passed=len(self.new_patterns) > 0,
            expected="> 0 new patterns",
            actual=str(len(self.new_patterns)),
            details=", ".join(sorted(self.new_patterns.keys())[:10]),
        )

    def _category_6_typo_learning_validation(self) -> None:
        category = "CATEGORY 6 — TYPO LEARNING SYSTEM"
        baseline = self._read_json(self.learned_spelling_file, {})

        typo = "battryy"
        for _ in range(self.config.spelling_promotion_threshold):
            self.engine.parse_title(f"Samsung A52 {typo}")

        learned = self._read_json(self.learned_spelling_file, {})
        actual = str(learned.get(typo, ""))
        self._record(
            category=category,
            case_id="learned_typo_battryy",
            title=f"Samsung A52 {typo}",
            passed=actual == "battery",
            expected="battery",
            actual=actual,
            details="learned_spelling_variations.json mapping check",
        )

        self.new_spelling_variations = {
            key: value
            for key, value in learned.items()
            if baseline.get(key) != value
        }
        self._record(
            category=category,
            case_id="new_typo_variations_count",
            title="learned_spelling_variations.json",
            passed=len(self.new_spelling_variations) > 0,
            expected="> 0 new spelling variations",
            actual=str(len(self.new_spelling_variations)),
            details=", ".join(
                f"{k}->{v}" for k, v in sorted(self.new_spelling_variations.items())[:10]
            ),
        )

    def _category_7_display_filtering(self) -> None:
        category = "CATEGORY 7 — DISPLAY FILTERING"
        titles = [
            "Galaxy A52 OLED Display Assembly",
            "Galaxy A50 LCD Screen Assembly",
        ]
        for idx, title in enumerate(titles, start=1):
            payload = self.engine.analyze_title(title)
            sku = str(payload.get("sku", ""))
            confidence = float(payload.get("confidence", 0.0) or 0.0)
            parser_reason = str(payload.get("reason", ""))
            passed = sku == NOT_UNDERSTANDABLE
            actual = "SKIPPED" if passed else sku
            self._record(
                category=category,
                case_id=f"display_filtered_{idx}",
                title=title,
                passed=passed,
                expected="SKIPPED",
                actual=actual,
                confidence=confidence,
                parser_reason=parser_reason,
            )

    def _category_8_lcd_connector_exception(self) -> None:
        category = "CATEGORY 8 — LCD CONNECTOR EXCEPTION"
        titles = [
            "Galaxy A52 LCD FPC Connector",
            "Galaxy A52 Display Connector Flex",
        ]
        for idx, title in enumerate(titles, start=1):
            payload = self.engine.analyze_title(title)
            sku = str(payload.get("sku", ""))
            confidence = float(payload.get("confidence", 0.0) or 0.0)
            parser_reason = str(payload.get("reason", ""))
            sku_tokens = set(sku.split())
            passed = sku != NOT_UNDERSTANDABLE and "FPC" in sku_tokens
            self._record(
                category=category,
                case_id=f"lcd_exception_{idx}",
                title=title,
                passed=passed,
                expected="Generated SKU including FPC",
                actual=sku,
                confidence=confidence,
                parser_reason=parser_reason,
            )

    def _category_9_confidence_scoring(self) -> None:
        category = "CATEGORY 9 — CONFIDENCE SCORING"
        bands = [
            (
                "confidence_exact",
                "Samsung Galaxy A52 A525 Charging Port",
                0.95,
                1.0,
                "Exact title match",
            ),
            (
                "confidence_corrected",
                "Samsng A52 Battry",
                0.85,
                0.95,
                "Corrected spelling",
            ),
            (
                "confidence_fuzzy",
                "Galaxy A52 Charge Conector Bord",
                0.70,
                0.86,
                "Fuzzy inference",
            ),
        ]

        for case_id, title, lower, upper, label in bands:
            payload = self.engine.analyze_title(title)
            confidence = float(payload.get("confidence", 0.0) or 0.0)
            parser_reason = str(payload.get("reason", ""))
            passed = lower <= confidence <= upper
            self._record(
                category=category,
                case_id=case_id,
                title=title,
                passed=passed,
                expected=f"{label} confidence in [{lower:.2f}, {upper:.2f}]",
                actual=f"{confidence:.4f}",
                confidence=confidence,
                parser_reason=parser_reason,
            )

    def _perf_title(self, idx: int) -> str:
        mod = idx % 8
        if mod == 0:
            return f"Samsung Galaxy A{20 + (idx % 80)} A{2000 + (idx % 5000)} Battery"
        if mod == 1:
            return f"Samsung Galaxy A{30 + (idx % 70)} A{3000 + (idx % 4000)} Charging Port"
        if mod == 2:
            return f"Samsung Galaxy A{10 + (idx % 90)} A{1000 + (idx % 6000)} Power Volume Flex"
        if mod == 3:
            return f"Google Pixel {6 + (idx % 5)} Pro Back Camera"
        if mod == 4:
            return f"Samsung Galaxy S{20 + (idx % 5)} S{900 + (idx % 100)} SIM Tray"
        if mod == 5:
            return f"iPhone {11 + (idx % 7)} Pro Max Charging Port Flex"
        if mod == 6:
            return f"Samsung Galaxy A{40 + (idx % 50)} A{2500 + (idx % 4500)} Ear Speaker Proximity Sensor"
        return f"Samsung Galaxy A{50 + (idx % 40)} A{3500 + (idx % 3000)} LCD FPC Connector"

    def _build_performance_dataset(self, size: int) -> pd.DataFrame:
        rows = [
            {
                "Product Name": self._perf_title(i),
                "Product SKU": f"PERF-SKU-{i}",
                "Product Web SKU": f"PERF-WEB-{i}",
            }
            for i in range(size)
        ]
        return pd.DataFrame(rows)

    def _category_10_performance(self) -> None:
        category = "CATEGORY 10 — PERFORMANCE TEST"
        for size in self.config.performance_sizes:
            input_path = self.workspace_dir / f"perf_{size}.csv"
            output_path = self.workspace_dir / f"perf_{size}_output.xlsx"
            review_path = self.workspace_dir / f"perf_{size}_review.xlsx"

            df = self._build_performance_dataset(size)
            df.to_csv(input_path, index=False)

            started = time.perf_counter()
            self.engine.process_inventory(input_path, output_path, review_path)
            elapsed = time.perf_counter() - started

            threshold = self.config.performance_threshold_seconds.get(size)
            passed = True if threshold is None else elapsed <= threshold
            throughput = size / elapsed if elapsed > 0 else float("inf")

            expected = (
                f"<= {threshold:.2f}s" if threshold is not None else "No threshold configured"
            )
            actual = f"{elapsed:.2f}s ({throughput:.1f} rows/sec)"
            self._record(
                category=category,
                case_id=f"performance_{size}",
                title=f"{size} titles",
                passed=passed,
                expected=expected,
                actual=actual,
                details=f"input={input_path.name}",
            )

    def run(self) -> dict[str, Any]:
        self._category_1_clean_titles()
        self._category_2_spelling_mistakes()
        self._category_3_multi_component_titles()
        self._category_4_flex_components()
        self._category_5_dataset_training_validation()
        self._category_6_typo_learning_validation()
        self._category_7_display_filtering()
        self._category_8_lcd_connector_exception()
        self._category_9_confidence_scoring()
        if self.config.include_performance:
            self._category_10_performance()
        return self.build_report()

    def build_report(self) -> dict[str, Any]:
        ordered_categories = list(self.category_results.keys())
        rows = [
            row
            for category in ordered_categories
            for row in self.category_results[category]
        ]

        total = len(rows)
        passed = sum(1 for row in rows if row.passed)
        failed = total - passed
        pass_rate = (passed / total * 100.0) if total else 0.0

        avg_conf: float | None = None
        if self._confidence_samples:
            avg_conf = round(sum(self._confidence_samples) / len(self._confidence_samples), 4)

        summary = ValidationSummary(
            total_tests_run=total,
            tests_passed=passed,
            tests_failed=failed,
            pass_rate=round(pass_rate, 2),
            average_confidence_score=avg_conf,
            new_patterns_learned=len(self.new_patterns),
            new_spelling_variations_learned=len(self.new_spelling_variations),
        )

        categories_payload = []
        for category in ordered_categories:
            results = self.category_results[category]
            category_total = len(results)
            category_passed = sum(1 for row in results if row.passed)
            categories_payload.append(
                {
                    "category": category,
                    "total": category_total,
                    "passed": category_passed,
                    "failed": category_total - category_passed,
                    "results": [asdict(row) for row in results],
                }
            )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "engine_config": {
                "include_performance": self.config.include_performance,
                "enable_vector_layer": self.config.enable_vector_layer,
                "pattern_min_frequency": self.config.pattern_min_frequency,
                "spelling_promotion_threshold": self.config.spelling_promotion_threshold,
                "performance_sizes": list(self.config.performance_sizes),
                "performance_threshold_seconds": self.config.performance_threshold_seconds,
            },
            "summary": asdict(summary),
            "categories": categories_payload,
            "new_patterns": self.new_patterns,
            "new_spelling_variations": self.new_spelling_variations,
            "artifacts": {
                "workspace_dir": str(self.workspace_dir),
                "learned_patterns_file": str(self.learned_patterns_file),
                "learned_spelling_variations_file": str(self.learned_spelling_file),
                "training_patterns_file": str(self.training_patterns_file),
            },
        }


def report_to_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# SKU Parser Validation Report",
        "",
        f"Generated at: `{report.get('generated_at', '')}`",
        "",
        "## Summary",
        "",
        f"- Total tests run: **{summary.get('total_tests_run', 0)}**",
        f"- Tests passed: **{summary.get('tests_passed', 0)}**",
        f"- Tests failed: **{summary.get('tests_failed', 0)}**",
        f"- Pass rate: **{summary.get('pass_rate', 0)}%**",
        f"- Average confidence score: **{summary.get('average_confidence_score')}**",
        f"- New patterns learned: **{summary.get('new_patterns_learned', 0)}**",
        f"- New spelling variations learned: **{summary.get('new_spelling_variations_learned', 0)}**",
        "",
        "## Category Results",
        "",
    ]

    for category_row in report.get("categories", []):
        lines.append(f"### {category_row.get('category', 'Unknown Category')}")
        lines.append(
            f"Passed {category_row.get('passed', 0)}/{category_row.get('total', 0)}"
        )
        lines.append("")
        lines.append("| Case | Passed | Expected | Actual | Confidence |")
        lines.append("|---|---|---|---|---|")
        for result in category_row.get("results", []):
            confidence = result.get("confidence")
            confidence_str = "" if confidence is None else f"{float(confidence):.4f}"
            passed_label = "YES" if result.get("passed") else "NO"
            lines.append(
                "| {case} | {passed} | {expected} | {actual} | {confidence} |".format(
                    case=result.get("case_id", ""),
                    passed=passed_label,
                    expected=str(result.get("expected", "")).replace("|", "\\|"),
                    actual=str(result.get("actual", "")).replace("|", "\\|"),
                    confidence=confidence_str,
                )
            )
        lines.append("")

    return "\n".join(lines)


def run_validation_suite(
    output_json: Path | None = None,
    output_markdown: Path | None = None,
    include_performance: bool = True,
    workspace_dir: Path | None = None,
    keep_workspace: bool = False,
    enable_vector_layer: bool = False,
    strict: bool = False,
    performance_sizes: tuple[int, ...] = (1000, 10000, 100000),
    performance_threshold_seconds: dict[int, float] | None = None,
) -> dict[str, Any]:
    base_dir = Path(__file__).resolve().parent
    config = FrameworkConfig(
        include_performance=include_performance,
        enable_vector_layer=enable_vector_layer,
        performance_sizes=performance_sizes,
        performance_threshold_seconds=(
            performance_threshold_seconds
            if performance_threshold_seconds is not None
            else FrameworkConfig().performance_threshold_seconds
        ),
    )

    if workspace_dir is not None:
        runner = SKUValidationFramework(
            base_dir=base_dir,
            workspace_dir=workspace_dir,
            config=config,
            reset_workspace=True,
        )
        report = runner.run()
    else:
        with tempfile.TemporaryDirectory(prefix="sku-validation-") as temp_dir:
            runner = SKUValidationFramework(
                base_dir=base_dir,
                workspace_dir=Path(temp_dir),
                config=config,
                reset_workspace=True,
            )
            report = runner.run()
            if keep_workspace:
                persisted = base_dir / ".validation_workspace"
                if persisted.exists():
                    shutil.rmtree(persisted)
                shutil.copytree(temp_dir, persisted)
                report.setdefault("artifacts", {})["workspace_dir"] = str(persisted)

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(report, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )

    if output_markdown is not None:
        output_markdown.parent.mkdir(parents=True, exist_ok=True)
        output_markdown.write_text(report_to_markdown(report), encoding="utf-8")

    failed = int(report.get("summary", {}).get("tests_failed", 0))
    if strict and failed > 0:
        raise RuntimeError(f"Validation failed with {failed} failing test(s).")

    return report


def _parse_int_csv(value: str) -> tuple[int, ...]:
    values = [segment.strip() for segment in value.split(",") if segment.strip()]
    if not values:
        raise ValueError("At least one size must be provided.")
    return tuple(int(segment) for segment in values)


def _parse_threshold_map(value: str) -> dict[int, float]:
    result: dict[int, float] = {}
    chunks = [segment.strip() for segment in value.split(",") if segment.strip()]
    for chunk in chunks:
        if ":" not in chunk:
            raise ValueError(f"Invalid threshold mapping: {chunk}")
        size_text, threshold_text = chunk.split(":", 1)
        result[int(size_text.strip())] = float(threshold_text.strip())
    return result


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run comprehensive validation for the SKU Parser Engine.",
    )
    parser.add_argument(
        "--output-json",
        default="outputs/sku_validation_report.json",
        help="Path to save JSON validation report.",
    )
    parser.add_argument(
        "--output-markdown",
        default="outputs/sku_validation_report.md",
        help="Path to save Markdown validation report.",
    )
    parser.add_argument(
        "--workspace-dir",
        default="",
        help="Optional working directory for generated artifacts.",
    )
    parser.add_argument(
        "--keep-workspace",
        action="store_true",
        help="Persist temporary workspace under .validation_workspace when no workspace-dir is provided.",
    )
    parser.add_argument(
        "--skip-performance",
        action="store_true",
        help="Skip category 10 performance tests.",
    )
    parser.add_argument(
        "--enable-vector-layer",
        action="store_true",
        help="Enable vector matching layer in validation engine.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with failure if any test fails.",
    )
    parser.add_argument(
        "--performance-sizes",
        default="1000,10000,100000",
        help="Comma-separated dataset sizes for performance tests.",
    )
    parser.add_argument(
        "--performance-thresholds",
        default="1000:5,10000:45,100000:180",
        help="Comma-separated size:seconds thresholds for performance pass/fail.",
    )
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    perf_sizes = _parse_int_csv(args.performance_sizes)
    perf_thresholds = _parse_threshold_map(args.performance_thresholds)

    report = run_validation_suite(
        output_json=Path(args.output_json),
        output_markdown=Path(args.output_markdown),
        include_performance=not args.skip_performance,
        workspace_dir=Path(args.workspace_dir) if args.workspace_dir else None,
        keep_workspace=bool(args.keep_workspace),
        enable_vector_layer=bool(args.enable_vector_layer),
        strict=bool(args.strict),
        performance_sizes=perf_sizes,
        performance_threshold_seconds=perf_thresholds,
    )

    summary = report.get("summary", {})
    print(
        "Validation complete: "
        f"{summary.get('tests_passed', 0)}/{summary.get('total_tests_run', 0)} passed, "
        f"{summary.get('tests_failed', 0)} failed, "
        f"avg confidence={summary.get('average_confidence_score')}"
    )
    print(f"JSON report: {args.output_json}")
    print(f"Markdown report: {args.output_markdown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
