"""Merge transformed dimensions with notebook-approved record links."""

from __future__ import annotations

import pandas as pd


def merge_grammy_spotify(
    awards_dimension: pd.DataFrame,
    spotify_dimension: pd.DataFrame,
    approved_matches: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve curated Grammy links to award keys and build the warehouse fact table."""
    required = {"year", "category", "nominee", "track_id"}
    missing = sorted(required - set(approved_matches.columns))
    if missing:
        raise ValueError(f"Curated match data is missing columns: {missing}")

    source = approved_matches.copy()
    source["year"] = pd.to_numeric(source["year"], errors="raise").astype("int64")
    source["track_id"] = source["track_id"].astype(str)

    award_lookup = awards_dimension[["award_key", "year", "category", "nominee"]]
    resolved = source.merge(
        award_lookup,
        on=["year", "category", "nominee"],
        how="left",
        validate="many_to_one",
    )
    if resolved["award_key"].isna().any():
        missing_count = int(resolved["award_key"].isna().sum())
        examples = resolved.loc[resolved["award_key"].isna(), ["year", "category", "nominee"]].head(5)
        raise ValueError(f"Could not resolve {missing_count} matches to Grammy awards: {examples.to_dict('records')}")

    valid_track_ids = set(spotify_dimension["track_id"].astype(str))
    resolved = resolved.loc[resolved["track_id"].isin(valid_track_ids)].copy()
    if resolved.empty:
        raise ValueError("No curated Grammy-Spotify matches reference a valid Spotify track")

    score_column = next(
        (column for column in ("score_linkage", "match_score", "puntaje_titulo") if column in resolved),
        None,
    )
    resolved["match_score"] = (
        pd.to_numeric(resolved[score_column], errors="coerce") if score_column else None
    )
    if "match_method" not in resolved:
        resolved["match_method"] = "NOTEBOOK_CURATED"
    resolved["match_method"] = resolved["match_method"].fillna("NOTEBOOK_CURATED").astype(str)

    fact = resolved[["award_key", "track_id", "match_method", "match_score"]]
    return (
        fact.sort_values("match_score", ascending=False, na_position="last")
        .drop_duplicates(["award_key", "track_id"], keep="first")
        .reset_index(drop=True)
    )
