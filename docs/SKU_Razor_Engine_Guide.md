# SKU Razor Engine Guide

Version: 1.0  
Date: March 6, 2026  
Audience: Developers, inventory team, operations team

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [How the System Works](#2-how-the-system-works)
3. [Text Normalization](#3-text-normalization)
4. [Model Detection](#4-model-detection)
5. [Part Detection](#5-part-detection)
6. [Attribute Extraction](#6-attribute-extraction)
7. [SKU Generation](#7-sku-generation)
8. [Duplicate Prevention](#8-duplicate-prevention)
9. [Data Training System](#9-data-training-system)
10. [Admin Training Dashboard](#10-admin-training-dashboard)
11. [Data Validation Rules](#11-data-validation-rules)
12. [Error Prevention](#12-error-prevention)
13. [Data Quality Requirement](#13-data-quality-requirement)
14. [Examples Section](#14-examples-section)
15. [Architecture Diagrams](#15-architecture-diagrams)

---

## 1. System Overview

The SKU Razor Engine is a smart parser for mobile repair inventory.

It reads messy product titles from suppliers and converts them into clean, standard SKUs.

### Why this exists

Different suppliers write titles in different ways.

Examples:

- `Fingerprint sensor galaxy a52 black`
- `Galaxy A52 finger print flex blk`
- `FP sensor for samsung a52`

These may all mean the same part.

The engine standardizes them so your inventory stays consistent.

### Example

Input title:

`Fingerprint Sensor for Samsung Galaxy A52 A525 Black`

Output SKU:

`GALAXY A52 A525 FS BLACK`

### Main business goal

One real part should have one correct SKU pattern.

Different real parts should not collapse into the same SKU.

---

## 2. How the System Works

The parser follows a clear pipeline.

```text
Product Title
  ↓
Text Normalization
  ↓
Noise Removal
  ↓
Model Detection
  ↓
Part Detection
  ↓
Attribute Extraction
  ↓
SKU Builder
  ↓
Validation + Confidence
  ↓
Final SKU
```

### Step summary in simple words

1. Clean the text so all titles are in the same style.
2. Remove words that do not help parsing.
3. Find brand and phone model.
4. Find part type from ontology and rules.
5. Find extra details like color and variant.
6. Build SKU in fixed order.
7. Check size and quality.
8. Return final SKU with confidence score.

---

## 3. Text Normalization

Before parsing, the engine cleans the input title.

### What the engine does

- Converts to lowercase for parsing logic.
- Removes punctuation and symbols.
- Replaces separators (`/`, `_`, `-`) with spaces.
- Fixes extra spaces.
- Applies phrase normalization and spelling correction.

### Example

Original title:

`Power / Volume Flex Cable for Galaxy A90`

Normalized title:

`power volume flex cable for galaxy a90`

### Why this is needed

If text is not normalized, the same part can be read as many different patterns.

Normalization makes matching reliable and repeatable.

---

## 4. Model Detection

The engine detects phone model using the global model database (`device_model_database.json`) and model aliases.

### Brands covered

Examples include:

- Apple
- Samsung
- Google
- Xiaomi
- Redmi
- Poco
- Oppo
- Vivo
- OnePlus
- Huawei
- Honor
- Realme
- Motorola
- Nokia
- Sony
- LG
- Tecno
- Infinix
- TCL
- ZTE
- Lenovo
- Asus

### Longest match rule

When model names share prefixes, the engine checks longest and most specific names first.

Example:

Input:

`pixel 8 pro ear speaker`

Correct model:

`PIXEL 8 PRO`

Not:

`PIXEL 8`

### Other examples

- `IPHONE 13 PRO MAX` is chosen before `IPHONE 13 PRO`.
- `GALAXY S23 ULTRA` is chosen before `GALAXY S23`.
- `REDMI NOTE 12 PRO PLUS` is chosen before `REDMI NOTE 12 PRO`.

### Pixel compatibility groups

For titles like `Pixel 6 / 6A / 6 Pro / 7 / 7 Pro`, the engine can keep a grouped model string for shared parts.

---

## 5. Part Detection

Part detection comes from the ontology and rule dictionaries.

Main files:

- `part_ontology.json`
- `mobile_parts_ontology.json`
- `part_code_rules.json`
- `camera_ontology.json`
- `speaker_ontology.json`
- `learned_patterns.json`

### Common mappings

- `fingerprint sensor` → `FS`
- `charging port` → `CP`
- `earpiece speaker` → `ES`
- `loud speaker` → `LS`
- `front camera` → `FC`
- `back camera` → `BC`
- `back camera lens` → `BCL`
- `battery` → `BATT`
- `sim tray` → `ST`

### Detection priority

The engine first tries deterministic rules and ontology phrases.

If no strong deterministic match is found, it can use fallback layers:

- fuzzy correction
- learned patterns
- semantic/vector fallback
- hint from SKU fields

This gives both accuracy and flexibility for messy supplier text.

---

## 6. Attribute Extraction

The engine extracts extra attributes that make SKUs unique.

### Color

From `color_dataset.json`, including synonyms.

Examples:

- `black`, `blk`, `jet black`, `stormy black` → `BLACK` or compressed color code based on active ontology rules.
- `sierra blue` → `SIERRA BLUE`

### Variant

Detected variants include:

- `BRKT` (bracket/holder/mount)
- `INT` (international version)
- `WF` / `NF` (with frame / no frame)
- `FRAME`
- `ADH`
- `MESH`

### Back door specific logic

Back door context can include lens and color details.

This is critical for avoiding duplicates.

### SIM tray mode

- `dual sim tray` → `STD`
- `single sim tray` → `ST`

### Why attributes matter

Without attributes, two physically different parts may produce the same SKU.

Attributes prevent this.

---

## 7. SKU Generation

The SKU builder creates output in fixed order.

```text
MODEL + MODEL_CODE + PART + VARIANT + COLOR
```

In some rule sets, part may include primary and secondary tokens.

### Examples

- `GALAXY A52 A525 FS BLACK`
- `PIXEL 8 PRO CP`
- `GALAXY A71 5G A716 NFC-CF BRKT`

### Hard rules

- SKU must be uppercase.
- Max length is 31 characters.
- If too long, engine trims safely.
- For some duplicate-prone cases, color is preserved even when long.

---

## 8. Duplicate Prevention

Duplicate prevention works in two ways.

### 1) Build-time enrichment

If part type can vary by color or variant, attributes are added.

Examples:

- back door
- sim tray
- fingerprint sensor

### 2) Batch duplicate resolution

When processing inventory files, engine checks duplicated output SKUs.

If duplicates are found, it enriches rows again with extra attributes (color/variant/part context) and rebuilds SKUs.

### Example

Before:

- `GALAXY A35 A356 BACKDOOR`
- `GALAXY A35 A356 BACKDOOR`

After enrichment:

- `GALAXY A35 A356 BACKDOOR BLACK`
- `GALAXY A35 A356 BACKDOOR WHITE`

---

## 9. Data Training System

The engine learns from new data over time.

### Training sources

- manual training entries from dashboard
- synonym training
- spelling correction training
- SKU correction training
- uploaded Excel/CSV datasets (title + correct SKU)
- auto-learned frequent patterns from batch parsing

### Learned files

- `learned_patterns.json`
- `learned_spelling_variations.json`
- `learned_sku_corrections.json`
- `training_examples.json`
- `learned_rules.json`
- `training_patterns.json`

### How training upload works

1. Upload dataset with title and correct SKU.
2. System parses each title.
3. Compare generated SKU vs correct SKU.
4. Save mismatch overrides for future parsing.
5. Reload parser resources.

This makes the system better with each approved training cycle.

---

## 10. Admin Training Dashboard

The dashboard is at `/training` (Next.js frontend + FastAPI admin endpoints).

It lets non-technical users train the parser in plain English forms.

### Main dashboard capabilities

1. Title Training Panel
- enter title and expected SKU
- saves training sample

2. Synonym Training
- map supplier phrase to standard term

3. Spelling Correction Training
- map wrong word to correct word

4. Part Ontology Manager
- add phrase to SKU code mapping

5. Color Training
- map supplier color to standard color

6. SKU Correction Panel
- map generated SKU to corrected SKU
- optional title-specific override

7. Dataset Training Upload
- upload `.xlsx`, `.xls`, or `.csv`
- auto-compare parsed vs expected SKUs

8. Rule Editor
- simple rule text like:
  - `If title contains 'back door' then part = BACKDOOR`

9. Training Analytics
- total parsed
- parsing accuracy
- duplicate SKUs
- unknown parts

10. Live Title Tester
- instant parse preview
- model, part, color, SKU, confidence, corrections

### Why dashboard matters

Inventory teams can improve parser behavior without writing code.

---

## 11. Data Validation Rules

This section defines validation standards for safe training operations.

### Core validation rules

- SKU must be unique for distinct physical parts.
- SKU length must be 31 characters or less.
- SKU must be uppercase.
- Part codes must exist in ontology.
- Model should exist in device model database.
- Uploaded dataset must include title and correct SKU columns.

### What happens on validation failure

- reject invalid training record
- send to review queue
- do not promote to learned files until fixed

### Current engine checks already present

- empty field checks for major training actions
- file type and column checks for dataset upload
- SKU normalization and uppercase formatting
- parser confidence and review flags
- duplicate indicators in batch output

---

## 12. Error Prevention

The system includes multiple safety layers.

### Error types and handling

1. Duplicate SKU risk
- duplicate scan in batch output
- enrichment pass adds missing attributes

2. Unknown part patterns
- low-confidence or unclear patterns logged in `unknown_parts_log.json`
- visible in analytics for review

3. Spelling mistakes
- dictionary + fuzzy + phonetic correction
- corrections returned in analysis response

4. Unknown or weak parse
- can return `NOT UNDERSTANDABLE TITLE`
- marked for review/manual action

5. AI fallback failures
- structured parser retries AI once
- if still invalid, returns controlled parse error path

---

## 13. Data Quality Requirement

The engine should only learn from clean, verified data.

### Quality policy

- every training entry must be reviewed and correct
- do not upload guessed SKUs
- do not approve unresolved unknown parts
- do not keep conflicting mappings for same phrase

### Reject dataset when it contains

- invalid SKU format
- duplicate SKU conflicts
- unknown part codes
- invalid model references
- obvious spelling noise that changes meaning

### Team workflow recommendation

1. Import dataset.
2. Review mismatches.
3. Approve only clean corrections.
4. Re-run test set before production use.

---

## 14. Examples Section

### Example 1

Title:

`Fingerprint Sensor for Galaxy A52 A525 Black`

SKU:

`GALAXY A52 A525 FS BLACK`

### Example 2

Title:

`Charging Port for Pixel 8 Pro`

SKU:

`PIXEL 8 PRO CP`

### Example 3

Title:

`Back Door for Galaxy A35 Black`

SKU:

`GALAXY A35 BACKDOOR BLACK`

### Example 4

Title:

`Wireless NFC Charging Flex with Bracket for Galaxy A71 5G A716`

SKU:

`GALAXY A71 5G A716 NFC-CF BRKT`

### Example 5

Title:

`pixel 8 pro ear spkeaker`

Detected corrections:

- `spkeaker -> speaker`

SKU:

`PIXEL 8 PRO ES`

---

## 15. Architecture Diagrams

### A) Main Data Flow

```text
Supplier Titles / Inventory File
            │
            ▼
     SKU Razor Engine
            │
            ├── Normalize text
            ├── Detect model
            ├── Detect part
            ├── Extract attributes
            ├── Build SKU
            └── Validate output
            │
            ▼
   Parsed SKU + Confidence + Flags
            │
            ├── Export to inventory sheet
            └── Send low-confidence rows to review queue
```

### B) Parser Pipeline (inside engine)

```text
Input title
  ↓
Normalization + token correction
  ↓
Brand/model detection (dataset + longest match)
  ↓
Part detection priority:
  rule → learned pattern → ontology → fuzzy → vector/semantic
  ↓
Combination rules (NFC-CF, PV-F, V/ES, etc.)
  ↓
Attribute extraction (variant, color, special context)
  ↓
SKU build + length check
  ↓
Duplicate enrichment pass (batch mode)
  ↓
Final SKU
```

### C) Training Pipeline

```text
Admin Dashboard / Upload Dataset
            │
            ▼
   TrainingDashboardService
            │
            ├── Validate input
            ├── Save updates to JSON datasets
            │      • learned_patterns.json
            │      • learned_spelling_variations.json
            │      • learned_sku_corrections.json
            │      • phrase_normalization.json
            │      • part_ontology.json
            │      • color_dataset.json
            ├── Reload parser runtime resources
            └── Update analytics counters
            │
            ▼
     Improved next parse cycle
```

---

## Appendix: Key Files

### Core Engine

- `backend/sku_intelligence_engine.py` (main parser and rule engine)
- `backend/sku_parser.py` (parser public interface)
- `backend/structured_sku_parser.py` (rule-first + AI fallback service)
- `backend/fastapi_app.py` (API endpoints)

### Training and Learning

- `backend/training_dashboard_service.py`
- `data/runtime/learned_patterns.json`
- `data/runtime/learned_spelling_variations.json`
- `data/runtime/learned_sku_corrections.json`
- `data/runtime/training_patterns.json`
- `data/runtime/training_examples.json`
- `data/runtime/learned_rules.json`

### Datasets

- `data/core/device_model_database.json`
- `data/core/brand_dataset.json`
- `data/core/part_ontology.json`
- `data/core/color_dataset.json`
- `data/core/camera_ontology.json`
- `data/core/speaker_ontology.json`
- `data/core/phrase_normalization.json`
- `data/core/spelling_corrections.json`
- `data/core/backdoor_patterns.json`

---

## End of Document

This guide is designed for both technical and non-technical teams.

If you update rules, always update this guide so operations and development stay aligned.
