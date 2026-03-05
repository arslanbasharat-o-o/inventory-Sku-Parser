#!/usr/bin/env python3
"""Production-grade SKU intelligence engine for mobile repair parts."""

from __future__ import annotations

import argparse
import difflib
import json
import math
import multiprocessing as mp
import os
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import openpyxl  # noqa: F401
import pandas as pd

try:
    from rapidfuzz import fuzz, process as rf_process

    RAPIDFUZZ_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    RAPIDFUZZ_AVAILABLE = False
    fuzz = None  # type: ignore[assignment]
    rf_process = None  # type: ignore[assignment]

try:
    from metaphone import doublemetaphone as _double_metaphone  # type: ignore[reportMissingImports]

    METAPHONE_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    METAPHONE_AVAILABLE = False

    def _double_metaphone(value: str) -> tuple[str, str]:
        return "", ""

try:
    import faiss  # type: ignore
    import numpy as np
    from sentence_transformers import SentenceTransformer  # type: ignore[reportMissingImports]

    VECTOR_LIBS_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    VECTOR_LIBS_AVAILABLE = False
    faiss = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]
    SentenceTransformer = None  # type: ignore[assignment]


NOT_UNDERSTANDABLE = "NOT UNDERSTANDABLE TITLE"
MAX_SKU_LENGTH = 31

PARTS_ONTOLOGY_FILE = Path("mobile_parts_ontology.json")
PARTS_DICTIONARY_FILE = Path("mobile_parts_dictionary.json")
PART_CODE_RULES_FILE = Path("part_code_rules.json")
LEARNED_TITLE_PATTERNS_FILE = Path("learned_title_patterns.json")
LEARNED_PARTS_FILE = Path("learned_parts.json")
LEARNED_PATTERNS_FILE = Path("learned_patterns.json")
TRAINING_PATTERNS_FILE = Path("training_patterns.json")
UNKNOWN_LOG_FILE = Path("unknown_parts_log.json")
SPELLING_CORRECTIONS_FILE = Path("spelling_corrections.json")
LEARNED_SPELLING_VARIATIONS_FILE = Path("learned_spelling_variations.json")

REQUIRED_INPUT_COLUMNS = ("Product Name", "Product SKU", "Product Web SKU")

BATTERY_ALIASES = {
    "battery",
    "batt",
    "battry",
    "batery",
    "battary",
    "bat",
}

DEFAULT_SPELLING_CORRECTIONS = {
    "samsng": "samsung",
    "samung": "samsung",
    "smasung": "samsung",
    "galaxi": "galaxy",
    "pixl": "pixel",
    "iphne": "iphone",
    "battry": "battery",
    "batery": "battery",
    "battary": "battery",
    "batry": "battery",
    "btry": "battery",
    "speker": "speaker",
    "speeker": "speaker",
    "ear speeker": "earpiece speaker",
    "ear speker": "earpiece speaker",
    "charng": "charging",
    "powr": "power",
    "volum": "volume",
    "camra": "camera",
    "try": "tray",
    "sim try": "sim tray",
    "vib": "vibrator",
    "motorr": "motor",
    "googel": "google",
    "goggle": "google",
    "pixle": "pixel",
    "charing": "charging",
    "conector": "connector",
    "vibretor": "vibrator",
}

DISPLAY_FILTER_PHRASES = {
    "lcd assembly",
    "oled assembly",
    "amoled assembly",
    "screen assembly",
}

DISPLAY_FILTER_EXCEPTIONS = {
    "lcd fpc connector",
    "display connector flex",
    "touch connector flex",
}

GENERIC_NOISE = {
    "for",
    "with",
    "without",
    "and",
    "or",
    "replacement",
    "replacing",
    "spare",
    "part",
    "parts",
    "mobile",
    "phone",
    "piece",
    "module",
    "assembly",
    "original",
    "new",
    "high",
    "quality",
}

MODEL_STOPWORDS = {
    "battery",
    "charging",
    "charge",
    "port",
    "connector",
    "board",
    "camera",
    "front",
    "back",
    "rear",
    "lens",
    "speaker",
    "earpiece",
    "loud",
    "sim",
    "tray",
    "flex",
    "cable",
    "nfc",
    "button",
    "power",
    "volume",
    "vibration",
    "vibrator",
    "vib",
    "motor",
    "display",
    "screen",
    "touch",
    "fpc",
    "mic",
    "microphone",
    "wifi",
    "antenna",
    "sensor",
    "proximity",
    "jack",
    "oled",
    "lcd",
    "amoled",
    "incell",
    "frame",
    "black",
    "white",
    "blue",
    "green",
    "silver",
    "gold",
    "purple",
    "pink",
    "gray",
    "grey",
    "single",
    "dual",
    "triple",
    "quad",
    "mini",
    "plus",
    "pro",
    "max",
    "ultra",
}

COLOR_CODES = {
    "black": "BLACK",
    "white": "WHITE",
    "blue": "BLUE",
    "green": "GREEN",
    "silver": "SILVER",
    "gold": "GOLD",
    "purple": "PURPLE",
    "pink": "PINK",
    "red": "RED",
    "gray": "GRAY",
    "grey": "GRAY",
    "yellow": "YELLOW",
}

VARIANT_CODES = {
    "dual": "DUAL",
    "triple": "TRI",
    "quad": "QUAD",
    "plus": "PLUS",
    "pro": "PRO",
    "max": "MAX",
    "mini": "MINI",
    "ultra": "ULTRA",
    "5g": "5G",
    "4g": "4G",
    "oled": "OLED",
    "amoled": "AMOLED",
    "incell": "INCELL",
    "without frame": "NF",
    "with frame": "WF",
}

PART_PRIORITY_TABLE: dict[str, int] = {
    "BATT": 100,
    "CP": 95,
    "CF": 90,
    "BC": 90,
    "FC": 90,
    "NFC": 91,
    "ST": 85,
    "STD": 85,
    "ES": 80,
    "LS": 80,
    "ES-PS": 82,
    "VIB": 75,
    "LIFT-MOT": 75,
    "ANNT-CONN": 70,
    "PV-F": 65,
    "PB-F": 65,
    "VOL-F": 65,
    "HJ": 60,
}

AMBIGUOUS_SINGLETON_PHRASES = {
    "connector",
    "flex",
    "board",
    "cable",
    "part",
    "parts",
    "assembly",
    "module",
}

# Canonical SKU family token emitted in SKU prefix.
BRAND_FAMILY_MAP = {
    "samsung": "GALAXY",
    "galaxy": "GALAXY",
    "apple": "IPHONE",
    "iphone": "IPHONE",
    "google": "PIXEL",
    "pixel": "PIXEL",
    "xiaomi": "MI",
    "mi": "MI",
    "redmi": "REDMI",
    "poco": "POCO",
    "oneplus": "ONEPLUS",
    "oppo": "OPPO",
    "vivo": "VIVO",
    "realme": "REALME",
    "huawei": "HUAWEI",
    "honor": "HONOR",
    "motorola": "MOTO",
    "moto": "MOTO",
    "infinix": "INFINIX",
    "tecno": "TECNO",
    "nokia": "NOKIA",
    "sony": "SONY",
    "asus": "ASUS",
    "lenovo": "LENOVO",
    "zte": "ZTE",
}

MANUFACTURER_LABEL_MAP = {
    "samsung": "SAMSUNG",
    "galaxy": "SAMSUNG",
    "apple": "APPLE",
    "iphone": "APPLE",
    "google": "GOOGLE",
    "pixel": "GOOGLE",
    "xiaomi": "XIAOMI",
    "mi": "XIAOMI",
    "redmi": "XIAOMI",
    "poco": "XIAOMI",
    "oneplus": "ONEPLUS",
    "oppo": "OPPO",
    "vivo": "VIVO",
    "realme": "REALME",
    "huawei": "HUAWEI",
    "honor": "HONOR",
    "motorola": "MOTOROLA",
    "moto": "MOTOROLA",
    "infinix": "INFINIX",
    "tecno": "TECNO",
    "nokia": "NOKIA",
    "sony": "SONY",
    "asus": "ASUS",
    "lenovo": "LENOVO",
    "zte": "ZTE",
}

DEFAULT_PART_CODE_RULES: dict[str, str] = {
    "power volume flex": "PV-F",
    "power and volume flex": "PV-F",
    "volume and power flex": "PV-F",
    "power + volume flex": "P/V-F",
    "power button flex": "PB-F",
    "volume button flex": "VOL-F",
    "power flex": "P-F",
    "camera flex": "CAM-F",
    "mic flex": "MIC-FC",
    "microphone flex": "MIC-FC",
    "mainboard flex cable": "MFC",
    "nfc flex": "NFC",
    "wifi antenna": "WIF-ANNT",
    "antenna connector": "ANNT-CONN",
    "charging port with headphone jack": "CP HJ",
    "charging port board with headphone jack": "CP HJ",
    "sim reader pcb": "SC-R-PCB-SR",
    "ear speaker proximity sensor": "ES-PS",
    "ear speaker and proximity sensor": "ES-PS",
    "vibration ear speaker": "V/ES",
    "vibration and ear speaker": "V/ES",
    "lift motor": "LIFT-MOT",
}

DEFAULT_MOBILE_PARTS_ONTOLOGY: dict[str, str] = {
    # Requested examples
    "battery": "BATT",
    "batt": "BATT",
    "battry": "BATT",
    "batery": "BATT",
    "battary": "BATT",
    "bat": "BATT",
    "charging port": "CP",
    "charging connector": "CP",
    "charging flex": "CF",
    "earpiece speaker": "ES",
    "loud speaker": "LS",
    "back camera": "BC",
    "front camera": "FC",
    "camera lens": "BCL",
    "vibrator": "VIB",
    "sim tray": "ST",
    "dual sim tray": "STD",
    "antenna connector": "ANNT-CONN",
    "wifi antenna": "WIF-ANNT",
    "power volume flex": "PV-F",
    "power button flex": "PB-F",
    "volume flex": "VOL-F",
    "camera flex": "CAM-F",
    "mic flex": "MIC-FC",
    "mainboard flex cable": "MFC",
    "nfc flex": "NFC",
    # Extended seed coverage
    "charging board": "CP",
    "charging socket": "CP",
    "charge connector": "CP",
    "charger port": "CP",
    "charging port flex": "CF",
    "wireless charging flex": "CF",
    "wireless nfc charging flex": "NFC CF",
    "battery connector": "BATT CON",
    "battery fpc connector": "BATT FPC",
    "display connector flex": "FPC",
    "lcd fpc connector": "FPC",
    "touch connector flex": "FPC",
    "fpc connector": "FPC",
    "sim reader": "SC-R",
    "sim card tray": "ST",
    "single sim tray": "ST",
    "ear speaker": "ES",
    "ear speaker proximity sensor": "ES-PS",
    "ear speaker and proximity sensor": "ES-PS",
    "vibration ear speaker": "V/ES",
    "vibration and ear speaker": "V/ES",
    "receiver speaker": "ES",
    "loudspeaker": "LS",
    "back camera lens": "BCL",
    "rear camera lens": "BCL",
    "rear camera": "BC",
    "main camera": "BC",
    "selfie camera": "FC",
    "front cam": "FC",
    "vibration motor": "VIB",
    "vibrator motor": "VIB",
    "lift motor": "LIFT-MOT",
    "vibrator flex": "VB-F",
    "vibration flex": "VB-F",
    "loudspeaker flex": "L-FLEX",
    "headphone jack": "HJ",
    "audio jack": "HJ",
    "earphone jack": "HJ",
    "proximity sensor": "PS",
    "proximity flex": "PS",
    "flashlight flex": "FLF",
    "fingerprint sensor": "FS",
    "fingerprint": "FR",
    "antenna cable": "ANT CON",
    "antenna connector cable": "ANT CON",
    "antenna connecting cable": "ANT CON",
    "back cover": "BACK DOOR",
    "back door": "BACK DOOR",
}

# Backward-compatible dictionary exposed to existing modules.
DEFAULT_MOBILE_PARTS_DICTIONARY: dict[str, str] = {
    **DEFAULT_MOBILE_PARTS_ONTOLOGY,
    **DEFAULT_PART_CODE_RULES,
}

SEMANTIC_TOKEN_HINTS = {
    "battery": "BATT",
    "batt": "BATT",
    "bat": "BATT",
    "charging": "CP",
    "port": "CP",
    "connector": "CP",
    "flex": "CF",
    "camera": "BC",
    "speaker": "LS",
    "earpiece": "ES",
    "sim": "ST",
    "tray": "ST",
    "vibration": "VIB",
    "vibrator": "VIB",
    "antenna": "ANT CON",
    "nfc": "NFC",
}

RE_SEPARATOR = re.compile(r"[_\-/]+")
RE_NON_ALNUM = re.compile(r"[^A-Za-z0-9\s+]")
RE_MULTI_SPACE = re.compile(r"\s+")
RE_TOKEN = re.compile(r"[a-z0-9+]+")
RE_SKU_CLEAN = re.compile(r"[^A-Z0-9/\-\s]")
RE_MODEL_CODE = re.compile(r"^[A-Z][0-9]{1,4}[A-Z]?$")
RE_ALPHA_NUM_TOKEN = re.compile(r"[A-Za-z0-9]+")
RE_WITHOUT_FRAME = re.compile(r"\b(without|no)\s+frame\b")
RE_WITH_FRAME = re.compile(r"\bwith\s+frame\b")

MAX_PARSE_CACHE_SIZE = 400_000
MULTIPROCESS_ROW_THRESHOLD = 120_000
MULTIPROCESS_CHUNK_SIZE = 2_000


@dataclass
class EngineConfig:
    ontology_file: Path = PARTS_ONTOLOGY_FILE
    dictionary_file: Path = PARTS_DICTIONARY_FILE
    part_rules_file: Path = PART_CODE_RULES_FILE
    learned_patterns_file: Path = LEARNED_PATTERNS_FILE
    legacy_learned_title_patterns_file: Path = LEARNED_TITLE_PATTERNS_FILE
    legacy_learned_parts_file: Path = LEARNED_PARTS_FILE
    unknown_log_file: Path = UNKNOWN_LOG_FILE
    training_patterns_file: Path = TRAINING_PATTERNS_FILE
    spelling_corrections_file: Path = SPELLING_CORRECTIONS_FILE
    learned_spelling_variations_file: Path = LEARNED_SPELLING_VARIATIONS_FILE
    max_sku_length: int = MAX_SKU_LENGTH
    unknown_promotion_threshold: int = 3
    spelling_promotion_threshold: int = 3
    pattern_min_frequency: int = 5
    pattern_ngram_min: int = 2
    pattern_ngram_max: int = 4
    token_fuzzy_threshold: int = 85
    token_difflib_cutoff: float = 0.85
    vector_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    enable_vector_layer: bool = True


@dataclass
class ParseResult:
    product_name: str
    suggested_sku: str
    confidence_score: float
    parser_reason: str
    decision: str
    brand: str
    model: str
    model_code: str
    part_code: str
    variant: str
    color: str


@dataclass
class BatchTrainingStats:
    rows_total: int
    rows_parsed: int
    learned_patterns: int
    promoted_patterns: int


class VectorMatcher:
    """Sentence-transformers + FAISS semantic matcher."""

    def __init__(self, model_name: str, enabled: bool = True) -> None:
        self.model_name = model_name
        self.enabled = enabled and VECTOR_LIBS_AVAILABLE
        self._model: Any = None
        self._index: Any = None
        self._payload: list[tuple[str, str]] = []

    def is_ready(self) -> bool:
        return self.enabled and self._index is not None and bool(self._payload)

    def build(self, phrase_to_code: dict[str, str]) -> None:
        if not self.enabled:
            return
        unique_items = [
            (phrase, code)
            for phrase, code in phrase_to_code.items()
            if phrase and code
        ]
        if not unique_items:
            return

        try:
            if self._model is None:
                self._model = SentenceTransformer(self.model_name)

            phrases = [phrase for phrase, _code in unique_items]
            embeddings = self._model.encode(
                phrases,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            if embeddings.size == 0:
                return
            dims = int(embeddings.shape[1])
            index = faiss.IndexFlatIP(dims)
            index.add(embeddings.astype("float32"))
            self._index = index
            self._payload = unique_items
        except Exception:
            self.enabled = False
            self._model = None
            self._index = None
            self._payload = []

    def query(self, text: str) -> tuple[str, float, str]:
        if not self.is_ready() or not text:
            return "", 0.0, ""

        try:
            embedding = self._model.encode(
                [text],
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            scores, indices = self._index.search(embedding.astype("float32"), 1)
            best_score = float(scores[0][0])
            best_idx = int(indices[0][0])
            if best_idx < 0 or best_idx >= len(self._payload):
                return "", 0.0, ""
            phrase, code = self._payload[best_idx]
            return code, max(0.0, min(1.0, best_score)), phrase
        except Exception:
            return "", 0.0, ""


class SKUIntelligenceEngine:
    """5-layer SKU intelligence system."""

    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()
        self._ensure_runtime_files()
        self._spelling_variation_counter: dict[tuple[str, str], int] = {}
        self.spelling_corrections = self._load_spelling_corrections()

        self.part_rules = self._load_part_rules()
        self.ontology = self._load_ontology()
        self.learned_patterns = self._load_learned_patterns()
        self.part_dictionary = self._build_part_dictionary()
        self.ontology_items = self._build_phrase_items(self.ontology)
        self.learned_pattern_items = self._build_phrase_items(self.learned_patterns)

        self.part_rule_items = self._build_phrase_items(self.part_rules)
        self.part_items = self._build_phrase_items(self.part_dictionary)
        self.part_phrase_list = [phrase for phrase, _code in self.part_items]
        self.known_codes = sorted(
            {code for _phrase, code in self.part_items},
            key=len,
            reverse=True,
        )

        self._component_vocab = self._build_component_vocabulary()
        self._component_vocab_list = tuple(sorted(self._component_vocab))
        self._soundex_index: dict[str, tuple[str, ...]] = {}
        self._metaphone_index: dict[str, tuple[str, ...]] = {}
        self._rebuild_phonetic_indexes()
        self._unknown_pattern_counter: dict[tuple[str, str], int] = {}
        self.vector_matcher = VectorMatcher(
            model_name=self.config.vector_model_name,
            enabled=self.config.enable_vector_layer,
        )
        self._rebuild_vector_index()

    def _ensure_runtime_files(self) -> None:
        self._ensure_json_dict_file(self.config.ontology_file, DEFAULT_MOBILE_PARTS_ONTOLOGY)
        self._ensure_json_dict_file(self.config.dictionary_file, DEFAULT_MOBILE_PARTS_DICTIONARY)
        self._ensure_json_dict_file(self.config.part_rules_file, DEFAULT_PART_CODE_RULES)
        self._ensure_json_dict_file(self.config.learned_patterns_file, {})
        self._ensure_json_dict_file(self.config.legacy_learned_title_patterns_file, {})
        self._ensure_json_dict_file(self.config.legacy_learned_parts_file, {})
        self._ensure_json_list_file(self.config.unknown_log_file, [])
        self._ensure_json_dict_file(self.config.training_patterns_file, {})
        self._ensure_json_dict_file(self.config.spelling_corrections_file, DEFAULT_SPELLING_CORRECTIONS)
        self._ensure_json_dict_file(self.config.learned_spelling_variations_file, {})

    def _ensure_json_dict_file(self, file_path: Path, seed: dict[str, str]) -> None:
        if file_path.exists():
            return
        file_path.write_text(json.dumps(seed, indent=2, ensure_ascii=True), encoding="utf-8")

    def _ensure_json_list_file(self, file_path: Path, seed: list[dict[str, object]]) -> None:
        if file_path.exists():
            return
        file_path.write_text(json.dumps(seed, indent=2, ensure_ascii=True), encoding="utf-8")

    def _load_json_dict(self, file_path: Path) -> dict[str, str]:
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        out: dict[str, str] = {}
        for key, value in data.items():
            phrase = self.normalize_phrase(key)
            code = self.normalize_code(value)
            if phrase and code:
                out[phrase] = code
        return out

    def _load_json_list(self, file_path: Path) -> list[dict[str, object]]:
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        return [row for row in data if isinstance(row, dict)]

    def _write_json(self, file_path: Path, payload: object) -> None:
        file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def _canonicalize_part_code(self, code: str) -> str:
        normalized = self.normalize_code(code)
        if not normalized:
            return ""
        if normalized == "BAT":
            return "BATT"
        if normalized.startswith("BAT "):
            return "BATT " + normalized[4:]
        return normalized

    def _load_spelling_corrections(self) -> dict[str, str]:
        merged: dict[str, str] = {}
        for source in (
            DEFAULT_SPELLING_CORRECTIONS,
            self._load_json_dict(self.config.spelling_corrections_file),
            self._load_json_dict(self.config.learned_spelling_variations_file),
        ):
            for typo, canonical in source.items():
                typo_key = self.normalize_phrase(typo)
                canonical_key = self.normalize_phrase(canonical)
                if typo_key and canonical_key:
                    merged[typo_key] = canonical_key
        for alias in BATTERY_ALIASES:
            merged[self.normalize_phrase(alias)] = "battery"
        return merged

    def _build_phrase_items(self, mapping: dict[str, str]) -> list[tuple[str, str]]:
        items = sorted(
            [(self.normalize_phrase(key), self._canonicalize_part_code(value)) for key, value in mapping.items()],
            key=lambda item: len(item[0]),
            reverse=True,
        )
        return [(phrase, code) for phrase, code in items if phrase and code]

    def _load_part_rules(self) -> dict[str, str]:
        merged = dict(DEFAULT_PART_CODE_RULES)
        merged.update(self._load_json_dict(self.config.part_rules_file))
        return {
            self.normalize_phrase(phrase): self._canonicalize_part_code(code)
            for phrase, code in merged.items()
            if self.normalize_phrase(phrase) and self._canonicalize_part_code(code)
        }

    def _load_ontology(self) -> dict[str, str]:
        merged = dict(DEFAULT_MOBILE_PARTS_ONTOLOGY)
        merged.update(self._load_json_dict(self.config.ontology_file))
        merged.update(self._load_json_dict(self.config.dictionary_file))
        for alias in BATTERY_ALIASES:
            merged[alias] = "BATT"
        return {
            self.normalize_phrase(phrase): self._canonicalize_part_code(code)
            for phrase, code in merged.items()
            if self.normalize_phrase(phrase) and self._canonicalize_part_code(code)
        }

    def _load_learned_patterns(self) -> dict[str, str]:
        merged: dict[str, str] = {}
        for source in (
            self._load_json_dict(self.config.learned_patterns_file),
            self._load_json_dict(self.config.legacy_learned_title_patterns_file),
            self._load_json_dict(self.config.legacy_learned_parts_file),
        ):
            for phrase, code in source.items():
                key = self.normalize_phrase(phrase)
                value = self._canonicalize_part_code(code)
                if key and value:
                    merged[key] = value
        return merged

    def _build_part_dictionary(self) -> dict[str, str]:
        merged = dict(self.ontology)
        merged.update(self.learned_patterns)
        return merged

    def _build_component_vocabulary(self) -> set[str]:
        vocab: set[str] = set()
        for phrase in self.part_dictionary:
            vocab.update(phrase.split())
        vocab.update(BRAND_FAMILY_MAP.keys())
        vocab.update(BRAND_FAMILY_MAP.values())
        vocab.update(MODEL_STOPWORDS)
        vocab.update(GENERIC_NOISE)
        vocab.update(COLOR_CODES.keys())
        vocab.update(VARIANT_CODES.keys())
        vocab.update(BATTERY_ALIASES)
        return {token.lower() for token in vocab if token}

    @staticmethod
    def _soundex(token: str) -> str:
        token = re.sub(r"[^a-z]", "", token.lower())
        if not token:
            return ""
        first = token[0].upper()
        mapping = {
            "b": "1",
            "f": "1",
            "p": "1",
            "v": "1",
            "c": "2",
            "g": "2",
            "j": "2",
            "k": "2",
            "q": "2",
            "s": "2",
            "x": "2",
            "z": "2",
            "d": "3",
            "t": "3",
            "l": "4",
            "m": "5",
            "n": "5",
            "r": "6",
        }
        out: list[str] = [first]
        prev = mapping.get(token[0], "")
        for ch in token[1:]:
            code = mapping.get(ch, "")
            if code and code != prev:
                out.append(code)
            prev = code
            if len(out) == 4:
                break
        while len(out) < 4:
            out.append("0")
        return "".join(out)

    def _phonetic_keys(self, token: str) -> set[str]:
        keys: set[str] = set()
        if not token or any(ch.isdigit() for ch in token):
            return keys
        if len(token) < 3:
            return keys
        sx = self._soundex(token)
        if sx:
            keys.add(f"SX:{sx}")
        if METAPHONE_AVAILABLE:
            p1, p2 = _double_metaphone(token)
            if p1:
                keys.add(f"MP:{p1}")
            if p2:
                keys.add(f"MP:{p2}")
        return keys

    def _rebuild_phonetic_indexes(self) -> None:
        soundex_map: dict[str, set[str]] = {}
        metaphone_map: dict[str, set[str]] = {}
        for token in self._component_vocab_list:
            if len(token) < 3 or any(ch.isdigit() for ch in token):
                continue
            for key in self._phonetic_keys(token):
                if key.startswith("SX:"):
                    soundex_map.setdefault(key, set()).add(token)
                elif key.startswith("MP:"):
                    metaphone_map.setdefault(key, set()).add(token)
        self._soundex_index = {
            key: tuple(sorted(values))
            for key, values in soundex_map.items()
        }
        self._metaphone_index = {
            key: tuple(sorted(values))
            for key, values in metaphone_map.items()
        }

    def _collect_phonetic_candidates(self, token: str) -> tuple[str, ...]:
        candidates: set[str] = set()
        for key in self._phonetic_keys(token):
            if key.startswith("SX:"):
                candidates.update(self._soundex_index.get(key, ()))
            elif key.startswith("MP:"):
                candidates.update(self._metaphone_index.get(key, ()))
        return tuple(sorted(candidates))

    def _apply_phrase_corrections(
        self,
        normalized_title: str,
    ) -> tuple[str, float, tuple[tuple[str, str], ...]]:
        if not normalized_title:
            return normalized_title, 1.0, ()
        phrase_map = {
            typo: canonical
            for typo, canonical in self.spelling_corrections.items()
            if " " in typo
        }
        if not phrase_map:
            return normalized_title, 1.0, ()

        corrected = normalized_title
        confidence = 1.0
        corrections: list[tuple[str, str]] = []
        for typo, canonical in sorted(phrase_map.items(), key=lambda item: len(item[0]), reverse=True):
            pattern = rf"\b{re.escape(typo)}\b"
            if re.search(pattern, corrected):
                corrected = re.sub(pattern, canonical, corrected)
                confidence = min(confidence, 0.92)
                corrections.append((typo, canonical))
        corrected = RE_MULTI_SPACE.sub(" ", corrected).strip()
        return corrected, confidence, tuple(corrections)

    @lru_cache(maxsize=50_000)
    def _correct_token_cached(self, token: str) -> tuple[str, float, str]:
        return self._correct_token_internal(token)

    def _correct_token_internal(self, token: str) -> tuple[str, float, str]:
        token = token.lower()
        if token in BATTERY_ALIASES:
            return "battery", 0.95, "battery_alias"

        mapped = self.spelling_corrections.get(token)
        if mapped:
            if mapped in BATTERY_ALIASES:
                return "battery", 0.93, "dictionary"
            return mapped, 0.93, "dictionary"

        if token in self._component_vocab:
            return token, 0.99, "exact"

        if len(token) < 2 or any(ch.isdigit() for ch in token):
            return token, 0.99, "model_or_short"

        if RAPIDFUZZ_AVAILABLE:
            best = rf_process.extractOne(
                token,
                self._component_vocab_list,
                scorer=fuzz.ratio,
                score_cutoff=int(self.config.token_fuzzy_threshold),
            )
            if best:
                candidate = str(best[0])
                score = float(best[1])
                if candidate in BATTERY_ALIASES:
                    candidate = "battery"
                conf = 0.80 + (score / 100.0) * 0.15
                return candidate, min(0.94, conf), "fuzzy"

        close = difflib.get_close_matches(
            token,
            self._component_vocab_list,
            n=1,
            cutoff=float(self.config.token_difflib_cutoff),
        )
        if close:
            candidate = close[0]
            if candidate in BATTERY_ALIASES:
                candidate = "battery"
            return candidate, 0.84, "difflib"

        phonetic_candidates = self._collect_phonetic_candidates(token)
        if phonetic_candidates:
            selected = ""
            if RAPIDFUZZ_AVAILABLE:
                phonetic_best = rf_process.extractOne(
                    token,
                    phonetic_candidates,
                    scorer=fuzz.ratio,
                    score_cutoff=70,
                )
                if phonetic_best:
                    selected = str(phonetic_best[0])
            if not selected:
                close_phonetic = difflib.get_close_matches(
                    token,
                    phonetic_candidates,
                    n=1,
                    cutoff=0.70,
                )
                if close_phonetic:
                    selected = close_phonetic[0]
            if selected:
                if selected in BATTERY_ALIASES:
                    selected = "battery"
                return selected, 0.80, "phonetic"

        return token, 0.65, "unchanged"

    def _normalize_with_token_corrections(self, normalized_title: str, learn: bool = False) -> str:
        corrected, _confidence, _method, _corrections = self._normalize_with_token_corrections_scored(
            normalized_title,
            learn=learn,
        )
        return corrected

    def _normalize_with_token_corrections_scored(
        self,
        normalized_title: str,
        learn: bool = False,
    ) -> tuple[str, float, str, tuple[tuple[str, str], ...]]:
        phrase_corrected, phrase_confidence, phrase_corrections = self._apply_phrase_corrections(
            normalized_title
        )
        tokens = self.tokenize(phrase_corrected)
        if not tokens:
            return phrase_corrected, phrase_confidence, "exact", phrase_corrections

        corrected_tokens: list[str] = []
        confidences: list[float] = [phrase_confidence]
        methods: list[str] = []
        token_corrections: list[tuple[str, str]] = list(phrase_corrections)
        for token in tokens:
            corrected_token, token_conf, method = self._correct_token_cached(token)
            corrected_tokens.append(corrected_token)
            confidences.append(token_conf)
            methods.append(method)
            if corrected_token != token:
                token_corrections.append((token, corrected_token))
            if learn and corrected_token != token:
                self._maybe_learn_spelling_variation(token, corrected_token)

        corrected = " ".join(corrected_tokens).strip() or phrase_corrected
        min_confidence = min(confidences) if confidences else 1.0
        exactish_methods = {"exact", "model_or_short"}
        if all(method in exactish_methods for method in methods):
            correction_method = "exact"
        elif any(method == "dictionary" for method in methods):
            correction_method = "dictionary"
        elif any(method == "fuzzy" for method in methods):
            correction_method = "fuzzy"
        elif any(method == "phonetic" for method in methods):
            correction_method = "phonetic"
        elif any(method == "difflib" for method in methods):
            correction_method = "difflib"
        else:
            correction_method = "unchanged"
        deduped_corrections: list[tuple[str, str]] = []
        seen_pairs: set[tuple[str, str]] = set()
        for source, target in token_corrections:
            pair = (source.strip(), target.strip())
            if not pair[0] or not pair[1] or pair[0] == pair[1]:
                continue
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            deduped_corrections.append(pair)
        return corrected, min_confidence, correction_method, tuple(deduped_corrections)

    def _rebuild_vector_index(self) -> None:
        corpus: dict[str, str] = {}
        for phrase, code in self.part_items:
            corpus[phrase] = code
            corpus[f"phone {phrase} replacement piece"] = code
            corpus[f"mobile {phrase} spare part"] = code
        if self.learned_patterns:
            for phrase, code in self.learned_patterns.items():
                corpus[phrase] = code
        self.vector_matcher.build(corpus)

    @staticmethod
    def normalize_text(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).lower()
        text = RE_SEPARATOR.sub(" ", text)
        text = RE_NON_ALNUM.sub(" ", text)
        text = RE_MULTI_SPACE.sub(" ", text)
        return text.strip()

    @staticmethod
    def normalize_phrase(value: object) -> str:
        return SKUIntelligenceEngine.normalize_text(value)

    @staticmethod
    def normalize_code(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).upper()
        text = RE_SKU_CLEAN.sub(" ", text)
        text = RE_MULTI_SPACE.sub(" ", text)
        return text.strip()

    @staticmethod
    def tokenize(text: str) -> list[str]:
        return RE_TOKEN.findall(text.lower())

    @staticmethod
    def _contains_phrase(text: str, phrase: str) -> bool:
        if not text or not phrase:
            return False
        return f" {phrase} " in f" {text} "

    def _should_filter_display_assembly(self, normalized_title: str) -> bool:
        if not normalized_title:
            return False
        padded = f" {normalized_title} "
        for safe_phrase in DISPLAY_FILTER_EXCEPTIONS:
            if f" {safe_phrase} " in padded:
                return False
        for blocked_phrase in DISPLAY_FILTER_PHRASES:
            if f" {blocked_phrase} " in padded:
                return True
        return False

    def _detect_brand_model(self, normalized_title: str) -> tuple[str, str]:
        tokens = self.tokenize(normalized_title)
        if not tokens:
            return "", ""

        brand = ""
        brand_idx = -1
        for idx, token in enumerate(tokens):
            if token in BRAND_FAMILY_MAP:
                brand = BRAND_FAMILY_MAP[token]
                brand_idx = idx
                break

        model_tokens: list[str] = []
        search_tokens = tokens[brand_idx + 1 :] if brand_idx >= 0 else tokens
        for token in search_tokens:
            if token in MODEL_STOPWORDS or token in GENERIC_NOISE:
                if model_tokens:
                    break
                continue
            if token in BRAND_FAMILY_MAP:
                if model_tokens:
                    break
                continue
            if re.fullmatch(r"\d{1,4}", token) or re.fullmatch(r"[a-z]?\d{1,4}[a-z]?", token):
                model_tokens.append(token)
                if len(model_tokens) >= 2:
                    break
                continue
            if re.fullmatch(r"[a-z]{1,6}", token):
                model_tokens.append(token)
                if len(model_tokens) >= 3:
                    break
                continue
            if model_tokens:
                break

        model = " ".join(token.upper() for token in model_tokens)
        return brand, model

    def _extract_model_code(self, raw_title: str, model: str = "") -> str:
        if not raw_title:
            return ""

        model_tokens = {token.upper() for token in RE_ALPHA_NUM_TOKEN.findall(model)}
        candidates: list[tuple[int, str]] = []

        for token in RE_ALPHA_NUM_TOKEN.findall(raw_title):
            candidate = token.upper()
            if candidate in model_tokens:
                continue
            if not (3 <= len(candidate) <= 8):
                continue
            if candidate == "5G":
                continue
            if not (re.search(r"[A-Z]", candidate) and re.search(r"\d", candidate)):
                continue
            if not RE_MODEL_CODE.fullmatch(candidate):
                continue

            score = 0
            if re.match(r"^[A-Z]", candidate):
                score += 2
            if len(re.findall(r"\d", candidate)) >= 3:
                score += 2
            if len(candidate) >= 4:
                score += 1
            candidates.append((score, candidate))

        if not candidates:
            return ""
        candidates.sort(key=lambda row: row[0], reverse=True)
        return candidates[0][1]

    def _detect_variant(self, normalized_title: str) -> str:
        padded = f" {normalized_title} "
        tokens = self.tokenize(normalized_title)

        out: list[str] = []
        if RE_WITHOUT_FRAME.search(normalized_title):
            out.append("NF")
        elif RE_WITH_FRAME.search(normalized_title):
            out.append("WF")

        for phrase, code in VARIANT_CODES.items():
            if " " in phrase and f" {phrase} " in padded and code not in out:
                out.append(code)

        for token in tokens:
            code = VARIANT_CODES.get(token)
            if code and code not in out:
                out.append(code)

        return " ".join(out[:2])

    def _detect_color(self, normalized_title: str) -> str:
        tokens = self.tokenize(normalized_title)
        found = [COLOR_CODES[token] for token in tokens if token in COLOR_CODES]
        if not found:
            return ""
        return found[-1]

    def _layer1_rule_engine(self, normalized_title: str) -> tuple[str, str, float]:
        padded = f" {normalized_title} "
        for phrase, code in self.part_rule_items:
            if f" {phrase} " in padded:
                return code, "layer1_rule_engine", 0.98
        return "", "", 0.0

    def _layer2_ontology_lookup(self, normalized_title: str) -> tuple[str, str, float]:
        padded = f" {normalized_title} "
        for phrase, code in self.part_items:
            if f" {phrase} " in padded:
                return code, "layer2_ontology", 0.95
        return "", "", 0.0

    def _maybe_learn_spelling_variation(self, typo: str, canonical: str) -> None:
        if not typo or not canonical:
            return
        if canonical in BATTERY_ALIASES:
            canonical = "battery"
        if typo == canonical:
            return
        if any(ch.isdigit() for ch in typo):
            return
        if len(typo) < 3:
            return
        if typo in self.spelling_corrections:
            return

        key = (typo, canonical)
        self._spelling_variation_counter[key] = self._spelling_variation_counter.get(key, 0) + 1
        if self._spelling_variation_counter[key] < self.config.spelling_promotion_threshold:
            return

        learned = self._load_json_dict(self.config.learned_spelling_variations_file)
        if learned.get(typo) == canonical:
            return
        learned[typo] = canonical
        self._write_json(
            self.config.learned_spelling_variations_file,
            dict(sorted(learned.items())),
        )
        self.spelling_corrections[typo] = canonical
        self._correct_token_cached.cache_clear()
        self._parse_cached.cache_clear()

    def _layer3_fuzzy_interpreter(self, normalized_title: str) -> tuple[str, str, float]:
        corrected_title, correction_confidence, correction_method, _corrections = self._normalize_with_token_corrections_scored(
            normalized_title,
            learn=False,
        )
        if not corrected_title:
            return "", "", 0.0

        if corrected_title != normalized_title:
            code, reason, _ = self._layer2_ontology_lookup(corrected_title)
            if code:
                if correction_method == "dictionary":
                    return code, "layer3_dictionary_correction", max(0.90, correction_confidence)
                if correction_method == "phonetic":
                    return code, "layer3_phonetic_correction", max(0.80, correction_confidence)
                return code, "layer3_fuzzy_token_correction", max(0.84, correction_confidence)

        if not RAPIDFUZZ_AVAILABLE or not self.part_phrase_list:
            return "", "", 0.0

        fuzzy_match = rf_process.extractOne(
            normalized_title,
            self.part_phrase_list,
            scorer=fuzz.token_set_ratio,
            score_cutoff=83,
        )
        if not fuzzy_match:
            return "", "", 0.0

        phrase, score, _idx = fuzzy_match
        code = self.part_dictionary.get(str(phrase), "")
        if not code:
            return "", "", 0.0

        confidence = 0.70 + (float(score) / 100.0) * 0.22
        return code, "layer3_fuzzy_phrase", min(0.92, confidence)

    def _layer4_pattern_learning(self, normalized_title: str) -> tuple[str, str, float]:
        if not self.learned_patterns:
            return "", "", 0.0

        padded = f" {normalized_title} "
        for phrase, code in self._build_phrase_items(self.learned_patterns):
            if f" {phrase} " in padded:
                return code, "layer4_learned_pattern", 0.90

        if RAPIDFUZZ_AVAILABLE:
            patterns = list(self.learned_patterns.keys())
            fuzzy_match = rf_process.extractOne(
                normalized_title,
                patterns,
                scorer=fuzz.token_set_ratio,
                score_cutoff=86,
            )
            if fuzzy_match:
                phrase, score, _idx = fuzzy_match
                code = self.learned_patterns.get(str(phrase), "")
                if code:
                    confidence = 0.78 + (float(score) / 100.0) * 0.14
                    return code, "layer4_learned_fuzzy", min(0.90, confidence)

        return "", "", 0.0

    def _layer5_vector_matching(self, normalized_title: str) -> tuple[str, str, float]:
        if self.vector_matcher.is_ready():
            code, similarity, _phrase = self.vector_matcher.query(normalized_title)
            if code and similarity >= 0.58:
                confidence = 0.62 + min(0.30, similarity * 0.32)
                return code, "layer5_vector", min(0.90, confidence)

        # Keep semantic layer alive even when vector stack is unavailable.
        if RAPIDFUZZ_AVAILABLE and self.part_phrase_list:
            fuzzy_match = rf_process.extractOne(
                normalized_title,
                self.part_phrase_list,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=80,
            )
            if fuzzy_match:
                phrase, score, _idx = fuzzy_match
                code = self.part_dictionary.get(str(phrase), "")
                if code:
                    confidence = 0.60 + (float(score) / 100.0) * 0.20
                    return code, "layer5_vector_fallback", min(0.86, confidence)

        return "", "", 0.0

    def _part_priority(self, code: str) -> int:
        normalized = self._canonicalize_part_code(code)
        return PART_PRIORITY_TABLE.get(normalized, 50)

    def _add_detected_code(
        self,
        detected: dict[str, dict[str, Any]],
        code: str,
        source: str,
        position: int,
    ) -> None:
        normalized = self._canonicalize_part_code(code)
        if not normalized:
            return
        parts = [self._canonicalize_part_code(token) for token in normalized.split()]
        parts = [token for token in parts if token]
        if not parts:
            return

        for token in parts:
            row = detected.setdefault(
                token,
                {
                    "priority": self._part_priority(token),
                    "position": position if position >= 0 else 10_000,
                    "sources": set(),
                },
            )
            row["priority"] = max(int(row.get("priority", 50)), self._part_priority(token))
            if position >= 0:
                row["position"] = min(int(row.get("position", 10_000)), position)
            cast_sources = row.setdefault("sources", set())
            if isinstance(cast_sources, set):
                cast_sources.add(source)
            else:
                row["sources"] = {source}

    def _apply_combination_rules(
        self,
        corrected_title: str,
        detected: dict[str, dict[str, Any]],
    ) -> None:
        tokens = set(self.tokenize(corrected_title))
        padded = f" {corrected_title} "

        if {"power", "volume", "flex"} <= tokens:
            pos = corrected_title.find("power")
            if "+" in tokens:
                self._add_detected_code(detected, "P/V-F", "combo_rule", pos)
                detected.pop("PV-F", None)
            else:
                self._add_detected_code(detected, "PV-F", "combo_rule", pos)
            for removable in ("PB-F", "VOL-F", "P-F", "CF"):
                detected.pop(removable, None)

        if (
            (" ear speaker " in padded or " earpiece speaker " in padded)
            and (" proximity sensor " in padded or {"proximity", "sensor"} <= tokens)
        ):
            pos = corrected_title.find("ear")
            self._add_detected_code(detected, "ES-PS", "combo_rule", pos)
            for removable in ("ES", "PS"):
                detected.pop(removable, None)

        if {"wireless", "nfc", "charging", "flex"} <= tokens:
            pos = corrected_title.find("wireless")
            self._add_detected_code(detected, "NFC", "combo_rule", pos)
            self._add_detected_code(detected, "CF", "combo_rule", pos)
            detected.pop("CP", None)

        if {"charging", "port", "flex"} <= tokens:
            pos = corrected_title.find("charging")
            self._add_detected_code(detected, "CF", "combo_rule", pos)
            detected.pop("CP", None)

        if {"sim", "reader", "pcb"} <= tokens:
            pos = corrected_title.find("sim")
            self._add_detected_code(detected, "SC-R-PCB-SR", "combo_rule", pos)
            for removable in ("SC-R", "ST"):
                detected.pop(removable, None)

        if (
            " lcd fpc connector " in padded
            or " display connector flex " in padded
            or " touch connector flex " in padded
            or " fpc connector " in padded
        ):
            pos = corrected_title.find("connector")
            self._add_detected_code(detected, "FPC", "combo_rule", pos)
            detected.pop("CP", None)

        if (
            ({"charge", "connector", "board"} <= tokens)
            or ({"charging", "connector", "board"} <= tokens)
            or ({"charge", "connector"} <= tokens)
        ):
            pos = corrected_title.find("charge")
            if pos < 0:
                pos = corrected_title.find("charging")
            self._add_detected_code(detected, "CP", "combo_rule", pos)
            detected.pop("CF", None)

        if {"ear", "receiver"} <= tokens:
            pos = corrected_title.find("ear")
            self._add_detected_code(detected, "ES", "combo_rule", pos)

        if {"vibrator", "motor"} <= tokens:
            pos = corrected_title.find("vibrator")
            self._add_detected_code(detected, "VIB", "combo_rule", pos)

    def _spans_overlap(
        self,
        start: int,
        end: int,
        occupied: list[tuple[int, int]],
    ) -> bool:
        for occ_start, occ_end in occupied:
            if start < occ_end and occ_start < end:
                return True
        return False

    def _match_phrase_non_overlapping(
        self,
        text: str,
        phrase: str,
        occupied: list[tuple[int, int]],
    ) -> int:
        if not phrase:
            return -1
        cursor = 0
        while True:
            pos = text.find(phrase, cursor)
            if pos < 0:
                return -1
            end = pos + len(phrase)
            left_ok = pos == 0 or text[pos - 1] == " "
            right_ok = end == len(text) or text[end] == " "
            if left_ok and right_ok and not self._spans_overlap(pos, end, occupied):
                occupied.append((pos, end))
                return pos
            cursor = pos + 1

    def _combine_parts_by_priority(
        self,
        corrected_title: str,
        normalized_hint: str,
    ) -> tuple[str, str, float]:
        detected: dict[str, dict[str, Any]] = {}
        occupied_spans: list[tuple[int, int]] = []

        deterministic_detected = False
        for phrase, code in self.part_rule_items:
            match_pos = self._match_phrase_non_overlapping(corrected_title, phrase, occupied_spans)
            if match_pos >= 0:
                self._add_detected_code(
                    detected,
                    code,
                    "rule",
                    match_pos,
                )
                deterministic_detected = True

        for phrase, code in self.learned_pattern_items:
            if len(phrase.split()) == 1 and phrase in AMBIGUOUS_SINGLETON_PHRASES:
                continue
            match_pos = self._match_phrase_non_overlapping(corrected_title, phrase, occupied_spans)
            if match_pos >= 0:
                self._add_detected_code(
                    detected,
                    code,
                    "learned",
                    match_pos,
                )
                deterministic_detected = True

        for phrase, code in self.ontology_items:
            if len(phrase.split()) == 1 and phrase in AMBIGUOUS_SINGLETON_PHRASES:
                continue
            match_pos = self._match_phrase_non_overlapping(corrected_title, phrase, occupied_spans)
            if match_pos >= 0:
                self._add_detected_code(
                    detected,
                    code,
                    "ontology",
                    match_pos,
                )
                deterministic_detected = True

        self._apply_combination_rules(corrected_title, detected)

        if not deterministic_detected:
            fuzzy_code, _fuzzy_reason, _fuzzy_conf = self._layer3_fuzzy_interpreter(corrected_title)
            if fuzzy_code:
                self._add_detected_code(detected, fuzzy_code, "fuzzy", 9_500)

            learned_code, _learned_reason, _learned_conf = self._layer4_pattern_learning(corrected_title)
            if learned_code:
                self._add_detected_code(detected, learned_code, "learned", 9_400)

            vector_code, _vector_reason, _vector_conf = self._layer5_vector_matching(corrected_title)
            if vector_code:
                self._add_detected_code(detected, vector_code, "vector", 9_300)

            semantic_code, _semantic_reason, _semantic_conf = self._semantic_token_hint(corrected_title)
            if semantic_code:
                self._add_detected_code(detected, semantic_code, "semantic", 9_200)

            hint_code, _hint_reason, _hint_conf = self._infer_from_sku_hint(normalized_hint)
            if hint_code:
                self._add_detected_code(detected, hint_code, "hint", 9_800)

            self._apply_combination_rules(corrected_title, detected)

        if not detected:
            return "", "", 0.0

        ranked = sorted(
            detected.items(),
            key=lambda item: (
                -int(item[1].get("priority", 50)),
                int(item[1].get("position", 10_000)),
                item[0],
            ),
        )

        ordered_codes = [code for code, _meta in ranked]
        main_code = ordered_codes[0]
        secondary_codes = [code for code in ordered_codes[1:] if code != main_code]

        if "NFC" in ordered_codes and "CF" in ordered_codes and {"wireless", "nfc", "charging", "flex"} <= set(self.tokenize(corrected_title)):
            main_code = "NFC"
            secondary_codes = [code for code in secondary_codes if code != "NFC"]
            if "CF" in ordered_codes and "CF" not in secondary_codes:
                secondary_codes.insert(0, "CF")

        combined = " ".join([main_code, *secondary_codes]).strip()

        all_sources: set[str] = set()
        for _code, meta in ranked:
            sources = meta.get("sources", set())
            if isinstance(sources, set):
                all_sources |= sources

        if "rule" in all_sources or "combo_rule" in all_sources:
            base_conf = 0.97
        elif "ontology" in all_sources:
            base_conf = 0.95
        elif "fuzzy" in all_sources or "learned" in all_sources:
            base_conf = 0.88
        elif "vector" in all_sources:
            base_conf = 0.84
        elif "semantic" in all_sources:
            base_conf = 0.76
        elif "hint" in all_sources:
            base_conf = 0.86
        else:
            base_conf = 0.70

        reason = "priority_multi_part" if len(ordered_codes) > 1 else "priority_single_part"
        return self._canonicalize_part_code(combined), reason, min(0.99, base_conf)

    def _semantic_token_hint(self, normalized_title: str) -> tuple[str, str, float]:
        tokens = set(self.tokenize(normalized_title))
        if not tokens:
            return "", "", 0.0

        scores: Counter[str] = Counter()
        for token in tokens:
            code = SEMANTIC_TOKEN_HINTS.get(token)
            if code:
                scores[code] += 1

        if not scores:
            return "", "", 0.0
        code, count = scores.most_common(1)[0]
        conf = min(0.74, 0.56 + count * 0.06)
        return code, "semantic_token_hint", conf

    def _generic_component_fallback(self, normalized_title: str) -> tuple[str, str, float]:
        tokens = self.tokenize(normalized_title)
        if not tokens:
            return "", "", 0.0
        if any(token in {"part", "parts", "replacement", "piece", "module", "component"} for token in tokens):
            return "GEN", "generic_component_fallback", 0.45
        return "", "", 0.0

    def _infer_from_sku_hint(self, hints_text: str) -> tuple[str, str, float]:
        compact_hint = re.sub(r"[^A-Z0-9]", "", self.normalize_code(hints_text))
        if not compact_hint:
            return "", "", 0.0

        for code in self.known_codes:
            tokens = [re.sub(r"[^A-Z0-9]", "", token) for token in code.split()]
            tokens = [token for token in tokens if token]
            if not tokens:
                continue
            if all(token in compact_hint for token in tokens):
                return code, "sku_hint_inference", 0.87
        return "", "", 0.0

    def _extract_unknown_phrase(self, normalized_title: str) -> str:
        tokens = [token for token in self.tokenize(normalized_title) if token not in GENERIC_NOISE]
        if not tokens:
            return ""

        cleaned: list[str] = []
        for token in tokens:
            if token in BRAND_FAMILY_MAP:
                continue
            if token in MODEL_STOPWORDS and token not in {"battery", "charging", "sim", "camera", "speaker", "antenna"}:
                continue
            if re.fullmatch(r"[a-z]?\d{1,4}[a-z]?", token):
                continue
            cleaned.append(token)

        if not cleaned:
            return ""

        return " ".join(cleaned[:4])

    def _update_unknown_log(self, title: str, suggested_code: str) -> None:
        phrase = self._extract_unknown_phrase(self.normalize_text(title))
        code = self.normalize_code(suggested_code)
        if not phrase or not code:
            return

        key = (phrase, code)
        self._unknown_pattern_counter[key] = self._unknown_pattern_counter.get(key, 0) + 1

        rows = self._load_json_list(self.config.unknown_log_file)
        for row in rows:
            if (
                str(row.get("title_pattern", "")) == phrase
                and str(row.get("suggested_code", "")) == code
            ):
                row["count"] = int(row.get("count", 0)) + 1
                self._write_json(self.config.unknown_log_file, rows)
                return

        rows.append({"title_pattern": phrase, "suggested_code": code, "count": 1})
        self._write_json(self.config.unknown_log_file, rows)

    def _compute_confidence(
        self,
        base: float,
        reason: str,
        brand: str,
        model: str,
        model_code: str,
        part_code: str,
    ) -> float:
        score = base
        if brand:
            score += 0.01
        if model:
            score += 0.02
        if model_code:
            score += 0.02
        if not brand and not model:
            score -= 0.08
        if not part_code:
            score = 0.0
        if reason == "layer5_vector_fallback":
            score -= 0.02
        if reason == "semantic_token_hint":
            score -= 0.07
        return round(max(0.0, min(1.0, score)), 4)

    def _decide(self, confidence: float) -> str:
        if confidence > 0.90:
            return "AUTO_ACCEPT"
        if confidence >= 0.70:
            return "REVIEW"
        return "MANUAL_VALIDATION"

    def _trim_sku(self, sku: str) -> str:
        sku = self.normalize_code(sku)
        if len(sku) <= self.config.max_sku_length:
            return sku

        kept: list[str] = []
        for token in sku.split():
            candidate = " ".join(kept + [token])
            if len(candidate) <= self.config.max_sku_length:
                kept.append(token)
            else:
                break

        if kept:
            return " ".join(kept)
        return sku[: self.config.max_sku_length].rstrip()

    def _build_sku(
        self,
        brand: str,
        model: str,
        model_code: str,
        part_code: str,
        variant: str,
        color: str,
    ) -> str:
        model_component = " ".join(token for token in (brand, model) if token).strip()
        tokens: list[str] = []

        if model_component:
            tokens.append(model_component)

        if model_code:
            model_tokens = {token.upper() for token in RE_ALPHA_NUM_TOKEN.findall(model_component.upper())}
            if model_code not in model_tokens:
                tokens.append(model_code)

        if part_code:
            tokens.extend(self.normalize_code(part_code).split())

        raw_sku = self.normalize_code(" ".join(tokens))
        return self._trim_sku(raw_sku)

    @lru_cache(maxsize=MAX_PARSE_CACHE_SIZE)
    def _parse_cached(
        self,
        product_name: str,
        product_sku: str,
        product_web_sku: str,
        product_description: str,
    ) -> tuple[str, float, str, str, str, str, str, str, str]:
        title_source = f"{product_name} {product_description}".strip()
        normalized_title = self.normalize_text(title_source)
        normalized_hint = self.normalize_text(f"{product_sku} {product_web_sku} {product_description}")
        corrected_title, correction_confidence, correction_method, _corrections = self._normalize_with_token_corrections_scored(
            normalized_title,
            learn=False,
        )

        if self._should_filter_display_assembly(normalized_title):
            return (
                NOT_UNDERSTANDABLE,
                1.0,
                "display_assembly_filtered",
                "",
                "",
                "",
                "",
                "",
                "",
            )

        brand, model = self._detect_brand_model(corrected_title)
        model_code = self._extract_model_code(title_source, model=f"{brand} {model}".strip())
        if not model_code and normalized_hint:
            model_code = self._extract_model_code(normalized_hint, model=f"{brand} {model}".strip())

        part_code, reason, base_conf = self._combine_parts_by_priority(
            corrected_title=corrected_title,
            normalized_hint=normalized_hint,
        )

        if not part_code:
            code, stage_reason, conf = self._generic_component_fallback(corrected_title)
            if code:
                part_code = code
                reason = stage_reason
                base_conf = conf

        part_code = self._canonicalize_part_code(part_code)
        if correction_confidence < 0.95:
            if correction_confidence >= 0.90:
                base_conf = max(0.0, base_conf - 0.03)
            elif correction_confidence >= 0.85:
                base_conf = max(0.0, base_conf - 0.08)
            else:
                base_conf = max(0.0, base_conf - 0.15)
        if reason and correction_method != "exact":
            reason = f"{reason}+{correction_method}"

        variant = self._detect_variant(corrected_title)
        color = self._detect_color(corrected_title)

        confidence = self._compute_confidence(
            base=base_conf,
            reason=reason,
            brand=brand,
            model=model,
            model_code=model_code,
            part_code=part_code,
        )

        sku = self._build_sku(
            brand=brand,
            model=model,
            model_code=model_code,
            part_code=part_code,
            variant=variant,
            color=color,
        )
        if not sku:
            sku = NOT_UNDERSTANDABLE

        return (
            sku,
            confidence,
            reason or "unresolved",
            brand,
            model,
            model_code,
            self.normalize_code(part_code),
            self.normalize_code(variant),
            self.normalize_code(color),
        )

    def parse_title(
        self,
        product_name: object,
        product_sku: object = "",
        product_web_sku: object = "",
        product_description: object = "",
    ) -> ParseResult:
        name_text = str(product_name or "")
        sku_hint_text = str(product_sku or "")
        web_hint_text = str(product_web_sku or "")
        description_text = str(product_description or "")
        combined_title_text = f"{name_text} {description_text}".strip()

        # Learn frequent typo variants outside cached path so counters stay accurate.
        self._normalize_with_token_corrections(self.normalize_text(combined_title_text), learn=True)

        (
            sku,
            confidence,
            reason,
            brand,
            model,
            model_code,
            part_code,
            variant,
            color,
        ) = self._parse_cached(name_text, sku_hint_text, web_hint_text, description_text)

        parsed = ParseResult(
            product_name=name_text,
            suggested_sku=sku,
            confidence_score=confidence,
            parser_reason=reason,
            decision=self._decide(confidence),
            brand=brand,
            model=model,
            model_code=model_code,
            part_code=part_code,
            variant=variant,
            color=color,
        )
        if parsed.confidence_score < 0.90 and parsed.part_code:
            self._update_unknown_log(name_text, parsed.part_code)
        return parsed

    def _detect_manufacturer_label(self, corrected_title: str) -> str:
        for token in self.tokenize(corrected_title):
            label = MANUFACTURER_LABEL_MAP.get(token.lower())
            if label:
                return label
        return ""

    def _split_primary_secondary_part(self, part_code: str) -> tuple[str, str]:
        normalized = self._canonicalize_part_code(part_code)
        if not normalized:
            return "", ""
        tokens = normalized.split()
        if not tokens:
            return "", ""
        primary = tokens[0]
        secondary = " ".join(tokens[1:]).strip()
        return primary, secondary

    def analyze_title(
        self,
        title: object,
        product_sku: object = "",
        product_web_sku: object = "",
        product_description: object = "",
    ) -> dict[str, object]:
        title_text = str(title or "")
        description_text = str(product_description or "")
        parsed = self.parse_title(title_text, product_sku, product_web_sku, description_text)

        normalized = self.normalize_text(f"{title_text} {description_text}".strip())
        corrected_title, _corr_conf, _corr_method, corrections = self._normalize_with_token_corrections_scored(
            normalized,
            learn=False,
        )

        primary_part, secondary_part = self._split_primary_secondary_part(parsed.part_code)
        model_component = " ".join(token for token in (parsed.brand, parsed.model) if token).strip()
        manufacturer = self._detect_manufacturer_label(corrected_title)
        if not manufacturer and parsed.brand:
            manufacturer = MANUFACTURER_LABEL_MAP.get(parsed.brand.lower(), parsed.brand)

        return {
            "title": title_text,
            "product_description": description_text,
            "normalized_title": normalized,
            "interpreted_title": corrected_title,
            "brand": manufacturer,
            "model": model_component,
            "model_code": parsed.model_code,
            "part": primary_part,
            "secondary_part": secondary_part,
            "sku": parsed.suggested_sku,
            "confidence": float(parsed.confidence_score),
            "decision": parsed.decision,
            "reason": parsed.parser_reason,
            "corrections": [
                {"from": source, "to": target}
                for source, target in corrections
            ],
            "parse_status": "parsed" if parsed.suggested_sku != NOT_UNDERSTANDABLE else "not_understandable",
        }

    def semantic_part_detection(self, title: object) -> str:
        result = self.parse_title(title)
        return result.part_code

    def _should_use_multiprocessing(self, unique_row_count: int) -> bool:
        if unique_row_count < MULTIPROCESS_ROW_THRESHOLD:
            return False
        if mp.cpu_count() <= 1:
            return False
        disable_mp = os.getenv("SKU_PARSER_DISABLE_MP", "").strip().lower()
        return disable_mp not in {"1", "true", "yes"}

    def _tokenize_title_for_pattern_learning(self, title: str) -> list[str]:
        normalized = self.normalize_text(title)
        corrected = self._normalize_with_token_corrections(normalized, learn=False)
        tokens = self.tokenize(corrected)
        if not tokens:
            return []

        brand_tokens = {token.lower() for token in BRAND_FAMILY_MAP}
        brand_tokens |= {token.lower() for token in BRAND_FAMILY_MAP.values()}

        filtered: list[str] = []
        for token in tokens:
            if token in brand_tokens:
                continue
            if token in GENERIC_NOISE:
                continue
            if re.fullmatch(r"[a-z]?\d{1,4}[a-z]?", token):
                continue
            filtered.append(token)
        return filtered

    def _iter_title_ngrams(self, tokens: list[str]) -> set[str]:
        if not tokens:
            return set()
        phrases: set[str] = set()
        min_n = max(1, int(self.config.pattern_ngram_min))
        max_n = max(min_n, int(self.config.pattern_ngram_max))
        token_count = len(tokens)
        for n in range(min_n, max_n + 1):
            if token_count < n:
                break
            for idx in range(0, token_count - n + 1):
                phrase = " ".join(tokens[idx : idx + n]).strip()
                if not phrase:
                    continue
                if all(token in GENERIC_NOISE for token in phrase.split()):
                    continue
                phrases.add(phrase)
        return phrases

    def _map_phrase_to_ontology_code(self, phrase: str) -> str:
        normalized_phrase = self.normalize_phrase(phrase)
        if not normalized_phrase:
            return ""

        ontology_lookup = {key: value for key, value in self.ontology_items}
        exact = ontology_lookup.get(normalized_phrase, "")
        if exact:
            return self._canonicalize_part_code(exact)

        phrase_tokens = set(normalized_phrase.split())
        best_code = ""
        best_score = -1.0
        best_len = -1
        for ontology_phrase, code in self.ontology_items:
            ontology_tokens = set(ontology_phrase.split())
            if not ontology_tokens:
                continue
            common = phrase_tokens & ontology_tokens
            if not common:
                continue
            overlap = len(common) / max(1, min(len(phrase_tokens), len(ontology_tokens)))
            if overlap < 0.67:
                continue
            if overlap > best_score or (math.isclose(overlap, best_score) and len(ontology_phrase) > best_len):
                best_score = overlap
                best_len = len(ontology_phrase)
                best_code = code

        if best_code:
            return self._canonicalize_part_code(best_code)

        ontology_phrases = [phrase_text for phrase_text, _code in self.ontology_items]
        if RAPIDFUZZ_AVAILABLE and ontology_phrases:
            fuzzy_match = rf_process.extractOne(
                normalized_phrase,
                ontology_phrases,
                scorer=fuzz.token_set_ratio,
                score_cutoff=90,
            )
            if fuzzy_match:
                ontology_phrase, _score, _idx = fuzzy_match
                code = ontology_lookup.get(str(ontology_phrase), "")
                if code:
                    return self._canonicalize_part_code(code)

        close = difflib.get_close_matches(
            normalized_phrase,
            ontology_phrases,
            n=1,
            cutoff=0.90,
        )
        if close:
            code = ontology_lookup.get(close[0], "")
            if code:
                return self._canonicalize_part_code(code)
        return ""

    def _extract_training_patterns(
        self,
        df: pd.DataFrame,
        result_df: pd.DataFrame,
    ) -> tuple[dict[str, str], BatchTrainingStats]:
        phrase_frequency: Counter[str] = Counter()
        phrase_code_votes: dict[str, Counter[str]] = defaultdict(Counter)
        model_pattern_counter: Counter[str] = Counter()
        abbreviations_counter: Counter[str] = Counter()

        for row in result_df[
            [
                "Product Name",
                "Parser Reason",
                "Part Code",
                "Confidence Score",
                "Product New SKU",
            ]
        ].fillna("").itertuples(index=False):
            title = str(row[0])
            reason = str(row[1])
            part_code = self.normalize_code(row[2])
            confidence = float(row[3] or 0.0)
            final_sku = str(row[4])
            tokens = self._tokenize_title_for_pattern_learning(title)
            ngrams = self._iter_title_ngrams(tokens)
            if ngrams:
                phrase_frequency.update(ngrams)

            if part_code and final_sku != NOT_UNDERSTANDABLE and ngrams:
                primary_code = self._canonicalize_part_code(part_code.split()[0])
                if primary_code:
                    for phrase in ngrams:
                        phrase_code_votes[phrase][primary_code] += 1

            model_code = self._extract_model_code(title)
            if model_code:
                model_pattern_counter[model_code] += 1

            if part_code and final_sku != NOT_UNDERSTANDABLE:
                abbreviations_counter[self._canonicalize_part_code(part_code)] += 1

        learned_updates: dict[str, str] = {}
        min_freq = max(1, int(self.config.pattern_min_frequency))
        for phrase, count in phrase_frequency.items():
            if count < min_freq:
                continue

            mapped_code = self._map_phrase_to_ontology_code(phrase)
            if not mapped_code and phrase in phrase_code_votes:
                code_counter = phrase_code_votes[phrase]
                total_votes = sum(code_counter.values())
                if total_votes > 0:
                    best_code, best_count = code_counter.most_common(1)[0]
                    if best_count / total_votes >= 0.85:
                        mapped_code = self._canonicalize_part_code(best_code)

            if not mapped_code:
                continue
            if mapped_code == "GEN":
                continue
            existing_code = self.learned_patterns.get(phrase, "")
            if existing_code == mapped_code:
                continue
            learned_updates[phrase] = mapped_code

        if learned_updates:
            self.learned_patterns.update(learned_updates)
            self._persist_learned_patterns()
            self.part_dictionary = self._build_part_dictionary()
            self.ontology_items = self._build_phrase_items(self.ontology)
            self.learned_pattern_items = self._build_phrase_items(self.learned_patterns)
            self.part_items = self._build_phrase_items(self.part_dictionary)
            self.part_phrase_list = [phrase for phrase, _code in self.part_items]
            self.known_codes = sorted(
                {code for _phrase, code in self.part_items},
                key=len,
                reverse=True,
            )
            self._component_vocab = self._build_component_vocabulary()
            self._component_vocab_list = tuple(sorted(self._component_vocab))
            self._rebuild_phonetic_indexes()
            self._correct_token_cached.cache_clear()
            self._rebuild_vector_index()
            self._parse_cached.cache_clear()

        top_patterns = []
        for phrase, count in sorted(
            phrase_frequency.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:200]:
            mapped_code = learned_updates.get(phrase) or self.learned_patterns.get(phrase) or self._map_phrase_to_ontology_code(phrase)
            top_patterns.append(
                {
                    "pattern": phrase,
                    "count": int(count),
                    "top_code": mapped_code or "",
                }
            )

        training_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rows_total": int(len(df)),
            "rows_parsed": int(result_df["Product New SKU"].astype(str).ne(NOT_UNDERSTANDABLE).sum()),
            "ngram_frequency_threshold": min_freq,
            "pattern_ngram_range": [int(self.config.pattern_ngram_min), int(self.config.pattern_ngram_max)],
            "frequent_phrase_candidates": int(sum(1 for _phrase, count in phrase_frequency.items() if count >= min_freq)),
            "common_title_patterns": top_patterns,
            "common_part_abbreviations": [
                {"code": code, "count": count}
                for code, count in abbreviations_counter.most_common(200)
            ],
            "model_detection_patterns": [
                {"model_code": code, "count": count}
                for code, count in model_pattern_counter.most_common(200)
            ],
            "promoted_to_learned_patterns": len(learned_updates),
        }
        self._write_json(self.config.training_patterns_file, training_payload)

        stats = BatchTrainingStats(
            rows_total=int(len(df)),
            rows_parsed=int(result_df["Product New SKU"].astype(str).ne(NOT_UNDERSTANDABLE).sum()),
            learned_patterns=int(len(self.learned_patterns)),
            promoted_patterns=len(learned_updates),
        )
        return learned_updates, stats

    def _persist_learned_patterns(self) -> None:
        sorted_patterns = dict(
            sorted(self.learned_patterns.items(), key=lambda item: (-len(item[0]), item[0]))
        )
        self._write_json(self.config.learned_patterns_file, sorted_patterns)

    def _review_queue_path(self, output_path: Path) -> Path:
        return output_path.with_name("review_queue.xlsx")

    def process_inventory(
        self,
        input_file: str | Path,
        output_file: str | Path,
        review_queue_file: str | Path | None = None,
    ) -> pd.DataFrame:
        input_path = Path(input_file)
        output_path = Path(output_file)

        if input_path.suffix.lower() == ".csv":
            df = pd.read_csv(input_path)
        else:
            df = pd.read_excel(input_path, engine="openpyxl")

        for col in REQUIRED_INPUT_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        key_df = df[list(REQUIRED_INPUT_COLUMNS)].fillna("").astype(str)
        unique_keys_df = key_df.drop_duplicates(ignore_index=True)
        unique_records = list(unique_keys_df.itertuples(index=False, name=None))

        counts_series = key_df.value_counts(sort=False) if unique_records else pd.Series(dtype=int)
        key_count_map = {tuple(key): int(count) for key, count in counts_series.items()} if unique_records else {}

        use_mp = self._should_use_multiprocessing(len(unique_records))
        parsed_map: dict[tuple[str, str, str], ParseResult] = {}

        if use_mp:
            worker_count = max(1, min(8, mp.cpu_count() - 1))
            try:
                ctx = mp.get_context("spawn")
                init_payload = {
                    **asdict(self.config),
                    "enable_vector_layer": False,
                }
                with ctx.Pool(
                    processes=worker_count,
                    initializer=_worker_init,
                    initargs=(init_payload,),
                ) as pool:
                    for record, payload in pool.imap_unordered(
                        _worker_parse,
                        unique_records,
                        chunksize=MULTIPROCESS_CHUNK_SIZE,
                    ):
                        parsed_map[record] = ParseResult(**payload)
            except Exception:
                parsed_map.clear()

        if not parsed_map:
            for record in unique_records:
                name, product_sku, web_sku = record
                parsed_map[record] = self.parse_title(name, product_sku, web_sku)

        row_keys = list(key_df.itertuples(index=False, name=None))
        parsed_rows = [parsed_map.get(key) for key in row_keys]

        output_df = df.copy()
        output_df["Product New SKU"] = [
            row.suggested_sku if row is not None else NOT_UNDERSTANDABLE
            for row in parsed_rows
        ]
        output_df["Confidence Score"] = [
            round(float(row.confidence_score), 4) if row is not None else 0.0
            for row in parsed_rows
        ]
        output_df["Parser Reason"] = [
            row.parser_reason if row is not None else "unresolved"
            for row in parsed_rows
        ]
        output_df["Parse Decision"] = [
            row.decision if row is not None else "MANUAL_VALIDATION"
            for row in parsed_rows
        ]
        output_df["Part Code"] = [
            row.part_code if row is not None else ""
            for row in parsed_rows
        ]

        # Duplicate detection (vectorized)
        normalized_new_sku = (
            output_df["Product New SKU"].fillna("").astype(str).str.strip().str.upper()
        )
        sku_dup_mask = normalized_new_sku.ne("") & normalized_new_sku.duplicated(keep=False)
        output_df["SKU Duplicate"] = sku_dup_mask.map({True: "DUPLICATED", False: ""})

        normalized_title = (
            output_df["Product Name"]
            .fillna("")
            .astype(str)
            .str.lower()
            .str.replace(r"[^a-z0-9\s]", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        title_key = normalized_title.map(lambda value: " ".join(sorted(value.split())) if value else "")
        title_dup_mask = title_key.ne("") & title_key.duplicated(keep=False)
        output_df["Title Duplicate"] = title_dup_mask.map({True: "DUPLICATED", False: ""})

        analysis_columns = [
            "Product Name",
            "Product SKU",
            "Product Web SKU",
            "Product New SKU",
            "Confidence Score",
            "Parser Reason",
            "Parse Decision",
            "Part Code",
            "SKU Duplicate",
            "Title Duplicate",
        ]
        analysis_df = output_df[analysis_columns].copy()

        # Maintain backward-compatible output schema used by existing API/UI tests.
        final_columns = [
            "Product Name",
            "Product SKU",
            "Product Web SKU",
            "Product New SKU",
            "SKU Duplicate",
            "Title Duplicate",
        ]
        final_df = analysis_df[final_columns].copy()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_excel(output_path, index=False, engine="openpyxl")

        review_path = Path(review_queue_file) if review_queue_file else self._review_queue_path(output_path)
        review_df = analysis_df[
            analysis_df["Confidence Score"].astype(float).lt(0.90)
        ][["Product Name", "Product New SKU", "Confidence Score", "Parser Reason"]].copy()
        review_df.columns = [
            "Product Name",
            "Suggested SKU",
            "Confidence Score",
            "Parser Reason",
        ]
        review_df.to_excel(review_path, index=False, engine="openpyxl")

        self._extract_training_patterns(df=key_df, result_df=analysis_df)
        return final_df


_WORKER_ENGINE: SKUIntelligenceEngine | None = None


def _worker_init(config_payload: dict[str, Any]) -> None:
    global _WORKER_ENGINE
    config = EngineConfig(**config_payload)
    _WORKER_ENGINE = SKUIntelligenceEngine(config=config)


def _worker_parse(record: tuple[str, str, str]) -> tuple[tuple[str, str, str], dict[str, Any]]:
    global _WORKER_ENGINE
    if _WORKER_ENGINE is None:
        _WORKER_ENGINE = SKUIntelligenceEngine()
    name, product_sku, web_sku = record
    parsed = _WORKER_ENGINE.parse_title(name, product_sku, web_sku)
    return record, asdict(parsed)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SKU Intelligence Engine")
    parser.add_argument("input_file", help="Input inventory file (.xlsx/.xls/.csv)")
    parser.add_argument(
        "-o",
        "--output",
        default="products_sku_processed.xlsx",
        help="Output file path for parsed SKU sheet.",
    )
    parser.add_argument(
        "--review-queue",
        default="review_queue.xlsx",
        help="Output file path for manual review queue.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    engine = SKUIntelligenceEngine()
    output_df = engine.process_inventory(args.input_file, args.output, args.review_queue)
    parsed = int(output_df["Product New SKU"].astype(str).ne(NOT_UNDERSTANDABLE).sum())
    print(
        json.dumps(
            {
                "rows_total": int(len(output_df)),
                "rows_parsed": parsed,
                "output_file": str(args.output),
                "review_queue_file": str(args.review_queue),
                "learned_patterns_file": str(engine.config.learned_patterns_file),
                "training_patterns_file": str(engine.config.training_patterns_file),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
