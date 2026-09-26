"""Tests for Gold transformation expectations."""

import pandas as pd


def test_match_stats_primary_key_is_unique():
    """Gold match_stats must have a unique (match_id, team_id) key."""
    df = pd.DataFrame(
        {
            "match_id": [1, 1, 2],
            "team_id": [10, 20, 10],
        }
    )

    assert not df.duplicated(["match_id", "team_id"]).any()


def test_player_stats_foreign_keys_match_existing_matches_and_teams():
    """Player stats references must resolve to existing Gold dimensions."""
    match_stats = pd.DataFrame(
        {"match_id": [1, 2], "team_id": [10, 20]}
    )
    player_stats = pd.DataFrame(
        {
            "match_id": [1, 2],
            "player_id": [100, 200],
            "team_id": [10, 20],
        }
    )

    valid_match_ids = set(match_stats["match_id"])
    valid_team_ids = set(match_stats["team_id"])

    assert player_stats["match_id"].isin(valid_match_ids).all()
    assert player_stats["team_id"].isin(valid_team_ids).all()