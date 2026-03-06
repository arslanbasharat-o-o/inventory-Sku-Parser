# Data Folder Structure

All JSON data is now organized in this `data` folder.

## `data/core`

Use this for **main datasets and rules** you want to maintain manually.

- `device_model_database.json`
- `brand_dataset.json`
- `part_ontology.json`
- `mobile_parts_ontology.json`
- `mobile_parts_dictionary.json`
- `part_code_rules.json`
- `spelling_corrections.json`
- `phrase_normalization.json`
- `color_dataset.json`
- `camera_ontology.json`
- `speaker_ontology.json`
- `backdoor_patterns.json`

When you want to update parser knowledge globally, edit files here.

## `data/runtime`

Use this for **auto-generated / learned** data.

- `learned_patterns.json`
- `learned_spelling_variations.json`
- `learned_sku_corrections.json`
- `learned_rules.json`
- `training_examples.json`
- `training_patterns.json`
- `unknown_parts_log.json`
- `learned_parts.json`
- `learned_title_patterns.json`

These files are created/updated by training and runtime flows.

## Optional Environment Variable

You can move data elsewhere by setting:

`SKU_DATA_DIR=/path/to/your/data`

If not set, default is this project’s `data` folder.
