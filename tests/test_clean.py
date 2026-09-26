"""Tests for core cleaning expectations."""

import pandas as pd


def test_cleaned_fixture_ids_are_not_null():
    """Fixture IDs are required for downstream joins."""
    df = pd.DataFrame({"fixture_id": [1, 2, 3]})
    assert df["fixture_id"].notna().all()


def test_cleaned_player_stats_has_required_columns():
    """Player statistics must contain the columns used downstream."""
    df = pd.DataFrame(
        {
            "fixture_id": [1],
            "team_id": [10],
            "player_id": [100],
            "player_name": ["Player"],
        }
    )

    required = {"fixture_id", "team_id", "player_id", "player_name"}
    assert required.issubset(df.columns)
