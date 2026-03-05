from __future__ import annotations

import pandas as pd

from train_title_patterns import train_patterns_from_dataframe


def test_train_patterns_learns_expected_part_phrases() -> None:
    df = pd.DataFrame(
        {
            "Product Name": [
                "Power Button Flex For Galaxy A52",
                "Power Button Flex Cable Samsung A52",
                "Charging Port Board For Galaxy A52",
                "Charging Connector For Galaxy A52",
                "Premium Battery Replacement For Galaxy A52",
                "Battery For Galaxy A52",
            ],
            "Product SKU": ["", "", "", "", "", ""],
            "Product Web SKU": ["", "", "", "", "", ""],
        }
    )

    learned, stats = train_patterns_from_dataframe(df, min_count=1, min_confidence=0.7)

    assert learned.get("power button flex") == "PB-F"
    assert learned.get("charging port") == "CP"
    assert learned.get("battery") == "BATT"
    assert stats["rows_used_for_learning"] == 6


def test_training_skips_brand_model_noise_phrases() -> None:
    df = pd.DataFrame(
        {
            "Product Name": [
                "SIM Tray For Galaxy S23",
                "SIM Tray For Galaxy S22",
                "SIM Tray For Galaxy S21",
            ],
            "Product SKU": ["", "", ""],
            "Product Web SKU": ["", "", ""],
        }
    )

    learned, _stats = train_patterns_from_dataframe(df, min_count=1, min_confidence=0.7)

    assert learned.get("sim tray") == "ST"
    assert "galaxy s23" not in learned
    assert "for galaxy" not in learned
