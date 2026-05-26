#!/usr/bin/env python3
"""Standalone post-hoc MLB CLV ranking diagnostics.

This script consumes existing CLV artifacts (clv_matches.csv) and optional
candidate-edge artifacts to evaluate whether score columns can act as a quality
filter before any Kelly/scaling changes.

No DB access, no model training, and no Gaussian-probability logic are used.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from scipy.stats import spearmanr
except Exception:  # pragma: no cover - optional dependency fallback
    spearmanr = None


REQUIRED_SCORING_TARGET = "clv_implied_prob"
CORE_SCORE_ORDER = (
    "raw_edge",
    "abs_edge",
    "model_prob",
    "implied_prob",
    "logit_edge",
    "model_prob_x_abs_edge",
    "edge_zscore",
)
OPTIONAL_SCORE_ORDER = (
    "line_score",
    "odds_at_bet_score",
)
CANDIDATE_SCORE_ORDER = (
    "candidate_best_edge",
    "candidate_mean_edge",
    "candidate_edge_survival_count",
    "candidate_edge_std",
    "reference_prob",
    "execution_prob",
    "model_alpha",
    "execution_alpha",
    "total_edge_decomposed",
    "selected_vs_candidate_best_gap",
    "selected_vs_candidate_mean_gap",
    "market_tightness_score",
    "quality_composite_v1",
)



def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _logit(series: pd.Series) -> pd.Series:
    clipped = np.clip(_coerce_numeric(series), 1e-6, 1 - 1e-6)
    return np.log(clipped / (1 - clipped))


def _zscore(series: pd.Series) -> pd.Series:
    arr = _coerce_numeric(series).astype(float)
    if arr.dropna().empty:
        return pd.Series([np.nan] * len(arr), index=series.index)
    mean = arr.mean()
    std = arr.std(ddof=0)
    if not pd.notna(std) or std == 0:
        return pd.Series([np.nan] * len(arr), index=series.index)
    return (arr - mean) / std


def _choose_implied_prob_column(df: pd.DataFrame) -> tuple[str | None, pd.Series]:
    if "implied_prob" in df.columns:
        implied = _coerce_numeric(df["implied_prob"])
        if implied.notna().any():
            return "implied_prob", implied
    if "bet_implied_prob" in df.columns:
        implied = _coerce_numeric(df["bet_implied_prob"])
        if implied.notna().any():
            return "bet_implied_prob", implied
    return None, pd.Series([np.nan] * len(df), index=df.index)


def _pick_edge_col(df: pd.DataFrame) -> str | None:
    for name in ("candidate_edge", "edge", "alt_edge", "book_edge"):
        if name in df.columns:
            if _coerce_numeric(df[name]).notna().any():
                return name
    return None


def _bookmaker_candidates(df: pd.DataFrame) -> str | None:
    if "bookmaker_at_bet" in df.columns:
        return "bookmaker_at_bet"
    if "bookmaker" in df.columns:
        return "bookmaker"
    return None


def _odds_column(df: pd.DataFrame) -> str | None:
    for col in ("odds_at_bet", "odds"):
        if col in df.columns:
            return col
    return None

def _line_column(df: pd.DataFrame) -> str | None:
    for col in ("line_at_bet", "line"):
        if col in df.columns:
            return col
    return None


def load_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _build_base_scores(df: pd.DataFrame) -> dict[str, pd.Series]:
    scores: dict[str, pd.Series] = {}
    if "edge" in df.columns:
        edge = _coerce_numeric(df["edge"])
        scores["raw_edge"] = edge
        scores["abs_edge"] = edge.abs()
        scores["edge_zscore"] = _zscore(edge)
        scores["model_prob_x_abs_edge"] = _coerce_numeric(df.get("model_prob")) * edge.abs() if "model_prob" in df.columns else pd.Series([np.nan] * len(df), index=df.index)
    else:
        zero = pd.Series([np.nan] * len(df), index=df.index)
        scores["raw_edge"] = zero
        scores["abs_edge"] = zero
        scores["edge_zscore"] = zero
        scores["model_prob_x_abs_edge"] = zero

    if "model_prob" in df.columns:
        scores["model_prob"] = _coerce_numeric(df["model_prob"])
    else:
        scores["model_prob"] = pd.Series([np.nan] * len(df), index=df.index)

    implied_col, implied = _choose_implied_prob_column(df)
    if implied_col is not None:
        scores["implied_prob"] = implied
    else:
        scores["implied_prob"] = pd.Series([np.nan] * len(df), index=df.index)

    model_prob = _coerce_numeric(df["model_prob"]) if "model_prob" in df.columns else pd.Series([np.nan] * len(df), index=df.index)
    if model_prob.notna().any() and scores["implied_prob"].notna().any():
        scores["logit_edge"] = _logit(model_prob) - _logit(scores["implied_prob"])
    else:
        scores["logit_edge"] = pd.Series([np.nan] * len(df), index=df.index)

    line_col = _line_column(df)
    if line_col is not None:
        line_series = _coerce_numeric(df[line_col])
        if line_series.notna().any():
            scores["line_score"] = line_series

    odds_col = _odds_column(df)
    if odds_col is not None:
        odds_series = _coerce_numeric(df[odds_col])
        if odds_series.notna().any():
            scores["odds_at_bet_score"] = odds_series

    # Normalize NaN-only scores out of the active set later.
    return scores


def _candidate_merge_key_options(clv: pd.DataFrame, candidates: pd.DataFrame) -> list[tuple[list[str], list[str]]]:
    options: list[tuple[list[str], list[str]]] = []

    def _have(cols: list[str]) -> bool:
        return all(c in clv.columns for c in cols) and all(c in candidates.columns for c in cols)

    if "bet_id" in clv.columns and "bet_id" in candidates.columns:
        options.append((["bet_id"], ["bet_id"]))

    for clv_line, cand_line in (
        ("line_at_bet", "line_at_bet"),
        ("line_at_bet", "line"),
        ("line", "line_at_bet"),
        ("line", "line"),
    ):
        clv_keys = ["player_id", "game_id", "market_key", clv_line]
        cand_keys = ["player_id", "game_id", "market_key", cand_line]
        if _have(cand_keys) and _have(clv_keys):
            options.append((clv_keys, cand_keys))

    for clv_line, cand_line in (("line_at_bet", "line"), ("line", "line")):
        clv_keys = ["player_id", "game_id", clv_line]
        cand_keys = ["player_id", "game_id", cand_line]
        if all(c in clv.columns for c in ["player_id", "game_id", clv_line]) and all(
            c in candidates.columns for c in ["player_id", "game_id", cand_line]
        ):
            options.append((clv_keys, cand_keys))
            break

    # Deduplicate while preserving order and first valid option precedence.
    unique_options: list[tuple[list[str], list[str]]] = []
    seen: set[str] = set()
    for clv_keys, cand_keys in options:
        key = ";".join(clv_keys) + "|" + ";".join(cand_keys)
        if key not in seen:
            unique_options.append((clv_keys, cand_keys))
            seen.add(key)

    return unique_options



def _coerce_key_for_merge(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            if pd.api.types.is_integer_dtype(out[col]):
                out[col] = out[col].astype("Int64")
            elif pd.api.types.is_float_dtype(out[col]):
                out[col] = pd.to_numeric(out[col], errors="coerce")
            else:
                out[col] = out[col].astype(str)
    return out


def load_candidate_features(clv_df: pd.DataFrame, candidate_edges_csv: str | Path | None) -> dict[str, pd.Series]:
    if not candidate_edges_csv:
        return {}

    path = Path(candidate_edges_csv)
    if not path.exists():
        return {}

    candidates = pd.read_csv(path)
    if candidates.empty:
        return {}

    for col in ["candidate_edge", "candidate_book", "book", "bookmaker", "odds", "edge"]:
        if col in candidates.columns and candidates[col].dtype == "O":
            pass

    edge_col = _pick_edge_col(candidates)
    if edge_col is None:
        return {}

    option_matches = _candidate_merge_key_options(clv_df, candidates)
    if not option_matches:
        return {}

    for clv_keys, cand_keys in option_matches:
        clv_key_cols = clv_keys
        cand_key_cols = cand_keys
        clv_subset = _coerce_key_for_merge(clv_df[[*clv_key_cols]].copy(), clv_key_cols)
        clv_subset = clv_subset.copy()
        clv_subset["__row_idx"] = np.arange(len(clv_subset))
        cand_subset = _coerce_key_for_merge(candidates[[*cand_key_cols, edge_col]].copy(), cand_key_cols)

        for ck in cand_key_cols:
            if ck not in cand_subset.columns:
                break
        else:
            candidate_edges = _coerce_numeric(cand_subset[edge_col])
            cand_subset = cand_subset.copy()
            cand_subset[edge_col] = candidate_edges
            if candidate_edges.notna().sum() == 0:
                continue

            agg = (
                cand_subset
                .groupby(cand_key_cols, dropna=False)
                .agg(
                    candidate_best_edge=(edge_col, "max"),
                    candidate_mean_edge=(edge_col, "mean"),
                    candidate_edge_std=(edge_col, "std"),
                    candidate_book_count=(edge_col, "count"),
                    candidate_edge_survival_count=(edge_col, lambda x: int((pd.to_numeric(x, errors="coerce") > 0).sum())),
                )
                .reset_index()
            )

            # For older candidate artifacts missing strict key coverage, prefer stable merge by
            # bet_id when available; otherwise use the first resolvable key set.
            merged = clv_subset.copy()
            merged["__row_idx"] = np.arange(len(merged))
            renamed = agg.rename(columns={c: f"__{c}" for c in agg.columns if c not in cand_key_cols})
            joined = clv_subset.merge(
                renamed,
                left_on=clv_key_cols,
                right_on=cand_key_cols,
                how="left",
            )
            if "__row_idx" not in joined.columns:
                continue

            out = pd.DataFrame(index=clv_df.index)
            out.loc[:, "candidate_best_edge"] = pd.to_numeric(joined.loc[:, "__candidate_best_edge"], errors="coerce").values
            out.loc[:, "candidate_mean_edge"] = pd.to_numeric(joined.loc[:, "__candidate_mean_edge"], errors="coerce").values
            out.loc[:, "candidate_edge_std"] = pd.to_numeric(joined.loc[:, "__candidate_edge_std"], errors="coerce").values
            out.loc[:, "candidate_book_count"] = pd.to_numeric(joined.loc[:, "__candidate_book_count"], errors="coerce").values
            out.loc[:, "candidate_edge_survival_count"] = pd.to_numeric(
                joined.loc[:, "__candidate_edge_survival_count"], errors="coerce"
            ).values

            selected_edge = _coerce_numeric(clv_df.get("edge")).reindex(clv_df.index)
            model_prob = _coerce_numeric(clv_df["model_prob"]) if "model_prob" in clv_df.columns else pd.Series([np.nan] * len(clv_df), index=clv_df.index)
            model_prob = model_prob.reindex(clv_df.index)
            out.loc[:, "selected_vs_candidate_best_gap"] = selected_edge - out["candidate_best_edge"]
            out.loc[:, "selected_vs_candidate_mean_gap"] = selected_edge - out["candidate_mean_edge"]
            out.loc[:, "reference_prob"] = model_prob - out["candidate_mean_edge"]
            out.loc[:, "execution_prob"] = model_prob - selected_edge
            out.loc[:, "model_alpha"] = model_prob - out["reference_prob"]
            out.loc[:, "execution_alpha"] = out["reference_prob"] - out["execution_prob"]
            out.loc[:, "total_edge_decomposed"] = out["model_alpha"] + out["execution_alpha"]
            out.loc[:, "alpha_reconstruction_error"] = selected_edge - out["total_edge_decomposed"]
            out.loc[:, "market_tightness_score"] = -out["candidate_edge_std"]

            implied_col, implied_prob = _choose_implied_prob_column(clv_df)
            if model_prob.notna().any() and implied_col is not None:
                raw_composite = _zscore(_logit(model_prob) - _logit(implied_prob))
            else:
                raw_composite = _zscore(selected_edge)
            # keep formula close to intent while being robust to missing data
            if out["candidate_edge_survival_count"].notna().any():
                raw_composite = raw_composite + _coerce_numeric(out["candidate_edge_survival_count"].fillna(0))
            if out["market_tightness_score"].notna().any():
                raw_composite = raw_composite + _coerce_numeric(out["market_tightness_score"].fillna(0))
            if out["selected_vs_candidate_mean_gap"].notna().any():
                raw_composite = raw_composite - out["selected_vs_candidate_mean_gap"].abs().fillna(0)
            out["quality_composite_v1"] = _zscore(raw_composite)

            return {
                k: pd.Series(v.values, index=clv_df.index)
                for k, v in out.items()
            }

    return {}


def build_score_registry(df: pd.DataFrame, *, candidate_scores: Mapping[str, pd.Series] | None = None) -> dict[str, pd.Series]:
    candidate_scores = candidate_scores or {}
    scores: dict[str, pd.Series] = _build_base_scores(df)
    for name, value in candidate_scores.items():
        scores[name] = value
    return scores


def filter_scores_for_set(score_map: Mapping[str, pd.Series], score_set: str) -> dict[str, pd.Series]:
    ordered = [
        *CORE_SCORE_ORDER,
        *OPTIONAL_SCORE_ORDER,
        *CANDIDATE_SCORE_ORDER,
    ]
    available = {}
    if score_set == "default":
        names = CORE_SCORE_ORDER
    elif score_set == "all":
        names = ordered
    else:
        raise ValueError("score_set must be 'default' or 'all'")

    for name in names:
        if name in score_map:
            available[name] = score_map[name]
    return available


def _block_columns(df: pd.DataFrame) -> tuple[str | None, str]:
    if "game_date" in df.columns:
        return "game_date", "game_date"
    if "game_id" in df.columns:
        return "game_id", "game_id"
    return None, "row"


def _spearman_score_clv(df: pd.DataFrame) -> float:
    x = _coerce_numeric(df["score"])
    y = _coerce_numeric(df[REQUIRED_SCORING_TARGET])
    xy = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(xy) < 3:
        return float("nan")
    if xy["x"].nunique() < 2 or xy["y"].nunique() < 2:
        return float("nan")
    if spearmanr is None:
        return float(xy["x"].corr(xy["y"], method="spearman"))
    corr = spearmanr(xy["x"], xy["y"])
    if corr is None or hasattr(corr, "pvalue") is False:  # pragma: no cover
        return float("nan")
    return float(getattr(corr, "statistic", float("nan")))


def _bootstrap_metric_ci(
    df: pd.DataFrame,
    metric_fn,
    n_resamples: int,
    ci_level: float,
    random_seed: int,
    block_col: str | None,
) -> dict[str, float | int | str]:
    cols = ["score", REQUIRED_SCORING_TARGET]
    if block_col and block_col in df.columns:
        cols.append(block_col)
    else:
        block_col = None
    clean = df[cols].copy()
    if clean.empty:
        return {
            "estimate": float("nan"),
            "ci_low": float("nan"),
            "ci_high": float("nan"),
            "n_blocks": 0,
            "bootstrap_method": "empty",
        }

    if block_col:
        if block_col in clean.columns:
            values = clean[block_col]
            if pd.api.types.is_datetime64_any_dtype(values):
                values = values.astype(str)
            clean = clean.assign(_block=values)
        else:
            block_col = None

    rng = np.random.default_rng(random_seed)
    if block_col:
        block_counts = clean.groupby("_block").size()
        blocks = [idx for idx, count in block_counts.items() if count > 0]
        grouped = {b: clean.loc[clean["_block"] == b] for b in blocks}
        if not blocks:
            block_col = None

    if not block_col:
        blocks = None
        n_blocks = len(clean)
        bootstrap_method = "row"
    else:
        n_blocks = len(blocks)
        bootstrap_method = "block"

    estimate = metric_fn(clean)
    vals = []
    for _ in range(n_resamples):
        if blocks is None:
            sampled = clean.iloc[rng.choice(len(clean), len(clean), replace=True)]
        else:
            sampled_blocks = rng.choice(blocks, size=len(blocks), replace=True)
            sampled = pd.concat([grouped[b] for b in sampled_blocks], ignore_index=True)
        vals.append(float(metric_fn(sampled)))

    vals_arr = np.asarray(vals, dtype=float)
    finite = vals_arr[np.isfinite(vals_arr)]
    alpha = (1.0 - ci_level) / 2.0
    if finite.size == 0:
        return {
            "estimate": float(estimate),
            "ci_low": float("nan"),
            "ci_high": float("nan"),
            "n_blocks": n_blocks,
            "bootstrap_method": bootstrap_method,
        }
    return {
        "estimate": float(estimate),
        "ci_low": float(np.quantile(finite, alpha)),
        "ci_high": float(np.quantile(finite, 1.0 - alpha)),
        "n_blocks": n_blocks,
        "bootstrap_method": bootstrap_method,
    }


def _score_bins_for_dataframe(
    df: pd.DataFrame,
    score_name: str,
    score_values: pd.Series,
    n_bootstrap: int,
    ci_level: float,
    random_seed: int,
) -> list[dict[str, Any]]:
    work = df.copy()
    work["score"] = score_values
    work = work.loc[work["score"].notna() & work[REQUIRED_SCORING_TARGET].notna()].copy()
    if work.empty:
        return []

    unique = work["score"].nunique(dropna=True)
    if unique < 2:
        return []

    n_bins = 10 if unique >= 10 and len(work) >= 100 else min(5, max(2, unique))
    try:
        work["bucket"] = pd.qcut(work["score"], q=n_bins, labels=False, duplicates="drop")
    except ValueError:
        return []
    if work["bucket"].isna().all():
        return []

    rows: list[dict[str, Any]] = []
    book_col = _bookmaker_candidates(df)
    for bucket, bucket_df in work.groupby("bucket", dropna=True):
        if bucket_df.empty:
            continue
        clv_vals = _coerce_numeric(bucket_df[REQUIRED_SCORING_TARGET]).dropna()
        if clv_vals.empty:
            mean_clv = float("nan")
            ci_low = float("nan")
            ci_high = float("nan")
            n_blocks = 0
            block_col, _ = _block_columns(bucket_df)
            bootstrap = {
                "estimate": float("nan"),
                "ci_low": float("nan"),
                "ci_high": float("nan"),
                "n_blocks": 0,
                "bootstrap_method": "empty",
            }
        else:
            block_col, _ = _block_columns(bucket_df)
            bucket_cols = ["score", REQUIRED_SCORING_TARGET]
            if block_col and block_col in bucket_df.columns:
                bucket_cols.append(block_col)
            bucket_work = bucket_df[bucket_cols].copy()
            bootstrap = _bootstrap_metric_ci(
                bucket_work,
                lambda d: float(_coerce_numeric(d[REQUIRED_SCORING_TARGET]).mean()) if len(d) else float("nan"),
                n_bootstrap,
                ci_level,
                random_seed,
                block_col,
            )
            mean_clv = float(clv_vals.mean())
            ci_low = bootstrap["ci_low"]
            ci_high = bootstrap["ci_high"]
            n_blocks = bootstrap["n_blocks"]

        same_book_share = None
        if "clv_source" in bucket_df.columns:
            same_book_share = float((bucket_df["clv_source"] == "same_book_close").sum() / len(bucket_df))

        top_bookmaker_share = None
        if book_col and book_col in bucket_df.columns:
            counts = bucket_df[book_col].replace("", pd.NA).dropna().value_counts()
            if not counts.empty:
                top_bookmaker_share = float(counts.iloc[0] / len(bucket_df))

        rows.append(
            {
                "score_name": score_name,
                "bucket": int(bucket),
                "bucket_n": int(len(bucket_df)),
                "mean_score": float(_coerce_numeric(bucket_df["score"]).mean()),
                "min_score": float(_coerce_numeric(bucket_df["score"]).min()),
                "max_score": float(_coerce_numeric(bucket_df["score"]).max()),
                "mean_clv": mean_clv,
                "clv_ci_low": ci_low,
                "clv_ci_high": ci_high,
                "n_blocks": int(n_blocks),
                "same_book_share": same_book_share,
                "top_bookmaker_share": top_bookmaker_share,
            }
        )
    return rows


def summarize_score(
    df: pd.DataFrame,
    score_name: str,
    score_values: pd.Series,
    *,
    bootstrap_samples: int,
    ci_level: float,
    min_n: int,
    random_seed: int,
) -> dict[str, Any]:
    work = df.copy()
    work["score"] = score_values
    valid = work.loc[work["score"].notna() & work[REQUIRED_SCORING_TARGET].notna()].copy()

    corr = _spearman_score_clv(valid)
    if valid.empty:
        return {
            "score_name": score_name,
            "n": int(len(work)),
            "n_scored": 0,
            "spearman": float("nan"),
            "ci_low": float("nan"),
            "ci_high": float("nan"),
            "n_blocks": 0,
            "monotonic_bins": False,
            "top_decile_mean_clv": float("nan"),
            "bottom_decile_mean_clv": float("nan"),
            "top_minus_bottom_clv": float("nan"),
            "pass": False,
            "bootstrap_method": "empty",
        }

    block_col, _ = _block_columns(valid)
    bootstrap_cols = ["score", REQUIRED_SCORING_TARGET]
    if block_col and block_col in valid.columns:
        bootstrap_cols.append(block_col)
    bootstrap = _bootstrap_metric_ci(
        valid[bootstrap_cols],
        _spearman_score_clv,
        bootstrap_samples,
        ci_level,
        random_seed,
        block_col,
    )

    bucket_rows = _score_bins_for_dataframe(
        df=valid,
        score_name=score_name,
        score_values=valid["score"],
        n_bootstrap=bootstrap_samples,
        ci_level=ci_level,
        random_seed=random_seed,
    )
    if bucket_rows:
        sorted_rows = sorted(bucket_rows, key=lambda r: r["bucket"])
        top_decile = float(sorted_rows[-1]["mean_clv"]) if sorted_rows else float("nan")
        bottom_decile = float(sorted_rows[0]["mean_clv"]) if sorted_rows else float("nan")
        top_minus_bottom = top_decile - bottom_decile
        monotonic = False
        if len(sorted_rows) >= 2 and all(row["mean_clv"] == row["mean_clv"] for row in sorted_rows):
            bucket_indices = np.arange(len(sorted_rows))
            bucket_means = np.asarray([row["mean_clv"] for row in sorted_rows], dtype=float)
            if np.isfinite(bucket_means).sum() >= 2 and bucket_means[0] < bucket_means[-1]:
                if spearmanr is None:
                    corr_bucket = np.corrcoef(bucket_indices, bucket_means)[0, 1]
                else:
                    corr_bucket = spearmanr(bucket_indices, bucket_means).statistic
                monotonic = bool(pd.notna(corr_bucket) and corr_bucket > 0)
    else:
        top_decile = float("nan")
        bottom_decile = float("nan")
        top_minus_bottom = float("nan")
        monotonic = False

    n_scored = len(valid)
    passes = bool(
        n_scored >= min_n
        and pd.notna(bootstrap.get("ci_low"))
        and bootstrap["ci_low"] > 0
        and pd.notna(top_minus_bottom)
        and top_minus_bottom > 0
        and monotonic
    )

    return {
        "score_name": score_name,
        "n": int(len(work)),
        "n_scored": int(n_scored),
        "spearman": float(corr),
        "ci_low": float(bootstrap["ci_low"]),
        "ci_high": float(bootstrap["ci_high"]),
        "n_blocks": int(bootstrap["n_blocks"]),
        "monotonic_bins": monotonic,
        "top_decile_mean_clv": top_decile,
        "bottom_decile_mean_clv": bottom_decile,
        "top_minus_bottom_clv": top_minus_bottom,
        "pass": passes,
        "bootstrap_method": bootstrap["bootstrap_method"],
    }


def build_slice_frames(
    df: pd.DataFrame,
    score_map: Mapping[str, pd.Series],
    *,
    bootstrap_samples: int,
    ci_level: float,
    min_n: int,
    random_seed: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    slices: list[tuple[str, pd.DataFrame]] = [("overall", df.copy())]

    book_col = _bookmaker_candidates(df)
    if book_col:
        for book in sorted(df[book_col].dropna().astype(str).unique()):
            slices.append((f"bookmaker={book}", df[df[book_col].astype(str) == book].copy()))

    odds_col = _odds_column(df)
    if odds_col and odds_col in df.columns:
        odds = _coerce_numeric(df[odds_col])
        plus = df.loc[odds >= 100].copy()
        not_plus = df.loc[(odds < 100) & odds.notna()].copy()
        if not plus.empty:
            slices.append(("plus_money", plus))
        if not not_plus.empty:
            slices.append(("not_plus_money", not_plus))

    for slice_name, slice_df in slices:
        for score_name, score_values in score_map.items():
            row = summarize_score(
                slice_df,
                score_name,
                score_values,
                bootstrap_samples=bootstrap_samples,
                ci_level=ci_level,
                min_n=min_n,
                random_seed=random_seed,
            )
            row["slice"] = slice_name
            if row["n_scored"] < min_n:
                row["pass"] = False
                row["underpowered"] = True
            else:
                row["underpowered"] = False
            rows.append(row)
    return rows


def _profit_stake_columns(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    profit = _coerce_numeric(df["profit"]) if "profit" in df.columns else pd.Series([np.nan] * len(df), index=df.index)
    stake = _coerce_numeric(df["stake"]) if "stake" in df.columns else pd.Series([np.nan] * len(df), index=df.index)
    return profit, stake


def _max_drawdown_from_profit(profit: pd.Series) -> float:
    clean = _coerce_numeric(profit).fillna(0.0).astype(float)
    if clean.empty:
        return float("nan")
    equity = clean.cumsum()
    running_max = equity.cummax().clip(lower=0.0)
    drawdown = running_max - equity
    return float(drawdown.max()) if not drawdown.empty else float("nan")


def _mean_clv_ci(
    df: pd.DataFrame,
    *,
    bootstrap_samples: int,
    ci_level: float,
    random_seed: int,
) -> dict[str, Any]:
    valid = df.loc[df[REQUIRED_SCORING_TARGET].notna()].copy()
    if valid.empty:
        return {"mean_clv": float("nan"), "clv_ci_low": float("nan"), "clv_ci_high": float("nan"), "n_blocks": 0}
    valid["score"] = 0.0
    block_col, _ = _block_columns(valid)
    cols = ["score", REQUIRED_SCORING_TARGET]
    if block_col and block_col in valid.columns:
        cols.append(block_col)
    boot = _bootstrap_metric_ci(
        valid[cols],
        lambda d: float(_coerce_numeric(d[REQUIRED_SCORING_TARGET]).mean()) if len(d) else float("nan"),
        bootstrap_samples,
        ci_level,
        random_seed,
        block_col,
    )
    return {
        "mean_clv": float(_coerce_numeric(valid[REQUIRED_SCORING_TARGET]).mean()),
        "clv_ci_low": float(boot["ci_low"]),
        "clv_ci_high": float(boot["ci_high"]),
        "n_blocks": int(boot["n_blocks"]),
    }


def _outcome_metrics(df: pd.DataFrame, *, bootstrap_samples: int, ci_level: float, random_seed: int) -> dict[str, Any]:
    profit, stake = _profit_stake_columns(df)
    profit_sum = float(profit.sum(skipna=True)) if profit.notna().any() else float("nan")
    stake_sum = float(stake.sum(skipna=True)) if stake.notna().any() else float("nan")
    roi = profit_sum / stake_sum if stake_sum and pd.notna(stake_sum) else float("nan")
    clv = _mean_clv_ci(df, bootstrap_samples=bootstrap_samples, ci_level=ci_level, random_seed=random_seed)
    return {
        "n": int(len(df)),
        "profit": profit_sum,
        "staked": stake_sum,
        "roi": float(roi),
        "max_drawdown": _max_drawdown_from_profit(profit),
        **clv,
    }


def _model_alpha_bucket(series: pd.Series) -> pd.Series:
    values = _coerce_numeric(series)
    out = pd.Series([pd.NA] * len(values), index=values.index, dtype="object")
    valid = values.dropna()
    if valid.empty:
        return out
    if valid.nunique() >= 3:
        try:
            labels = ["low", "medium", "high"]
            binned = pd.qcut(valid, q=3, labels=labels, duplicates="drop")
            out.loc[valid.index] = binned.astype("object")
            return out
        except ValueError:
            pass
    median = valid.median()
    out.loc[valid.index] = np.where(valid < median, "low", "high")
    return out


def _execution_alpha_bucket(series: pd.Series) -> pd.Series:
    values = _coerce_numeric(series)
    out = pd.Series([pd.NA] * len(values), index=values.index, dtype="object")
    out.loc[values < 0] = "negative"
    out.loc[(values >= 0) & (values < 0.005)] = "neutral"
    out.loc[values >= 0.005] = "positive"
    return out


def build_alpha_2d_buckets(
    df: pd.DataFrame,
    *,
    bootstrap_samples: int,
    ci_level: float,
    random_seed: int,
) -> list[dict[str, Any]]:
    if "model_alpha" not in df.columns or "execution_alpha" not in df.columns:
        return []
    work = df.copy()
    work["model_alpha_bucket"] = _model_alpha_bucket(work["model_alpha"])
    work["execution_alpha_bucket"] = _execution_alpha_bucket(work["execution_alpha"])
    work = work.loc[work["model_alpha_bucket"].notna() & work["execution_alpha_bucket"].notna()].copy()
    if work.empty:
        return []

    rows: list[dict[str, Any]] = []
    for (model_bucket, execution_bucket), bucket_df in work.groupby(["model_alpha_bucket", "execution_alpha_bucket"], dropna=True):
        metrics = _outcome_metrics(bucket_df, bootstrap_samples=bootstrap_samples, ci_level=ci_level, random_seed=random_seed)
        rows.append(
            {
                "model_alpha_bucket": str(model_bucket),
                "execution_alpha_bucket": str(execution_bucket),
                "bucket_n": int(len(bucket_df)),
                "mean_model_alpha": float(_coerce_numeric(bucket_df["model_alpha"]).mean()),
                "mean_execution_alpha": float(_coerce_numeric(bucket_df["execution_alpha"]).mean()),
                **metrics,
            }
        )
    order_model = {"low": 0, "medium": 1, "high": 2}
    order_exec = {"negative": 0, "neutral": 1, "positive": 2}
    return sorted(rows, key=lambda r: (order_model.get(r["model_alpha_bucket"], 99), order_exec.get(r["execution_alpha_bucket"], 99)))


def build_filter_replay(
    df: pd.DataFrame,
    *,
    bootstrap_samples: int,
    ci_level: float,
    random_seed: int,
) -> list[dict[str, Any]]:
    if "model_alpha" not in df.columns or "execution_alpha" not in df.columns:
        return []
    model_alpha = _coerce_numeric(df["model_alpha"])
    execution_alpha = _coerce_numeric(df["execution_alpha"])
    rules: list[tuple[str, pd.Series]] = [
        ("all", pd.Series([True] * len(df), index=df.index)),
        ("execution_alpha>=0", execution_alpha >= 0),
        ("execution_alpha>=0.005", execution_alpha >= 0.005),
        ("model_alpha>=0", model_alpha >= 0),
        ("model_alpha>=0.02", model_alpha >= 0.02),
        ("model_alpha>=0_and_execution_alpha>=0.005", (model_alpha >= 0) & (execution_alpha >= 0.005)),
        (
            "0<=model_alpha<=0.10_and_execution_alpha>0",
            (model_alpha >= 0) & (model_alpha <= 0.10) & (execution_alpha > 0),
        ),
    ]
    rows: list[dict[str, Any]] = []
    for name, mask in rules:
        subset = df.loc[mask.fillna(False)].copy()
        metrics = _outcome_metrics(subset, bootstrap_samples=bootstrap_samples, ci_level=ci_level, random_seed=random_seed)
        rows.append(
            {
                "filter_rule": name,
                "mean_model_alpha": float(_coerce_numeric(subset["model_alpha"]).mean()) if not subset.empty else float("nan"),
                "mean_execution_alpha": float(_coerce_numeric(subset["execution_alpha"]).mean()) if not subset.empty else float("nan"),
                **metrics,
            }
        )
    return rows


def _score_class(score_name: str) -> str:
    model_scores = {"raw_edge", "abs_edge", "model_prob", "logit_edge", "model_prob_x_abs_edge", "edge_zscore", "model_alpha", "total_edge_decomposed"}
    market_scores = {"candidate_best_edge", "candidate_mean_edge", "candidate_edge_survival_count", "candidate_edge_std", "selected_vs_candidate_best_gap", "selected_vs_candidate_mean_gap", "market_tightness_score", "execution_alpha", "reference_prob", "execution_prob"}
    if score_name in model_scores:
        return "model_edge"
    if score_name in market_scores:
        return "market_relative_price"
    return "other"


def build_scorecard_rows(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "score_class": _score_class(str(row.get("score_name", "")))} for row in summary_rows]


def build_markdown(
    output_path: Path,
    clv_matches_path: Path,
    summary_rows: list[dict[str, Any]],
    candidate_path: str | Path | None,
) -> None:
    n_rows = len(summary_rows)
    passed_rows = [r for r in summary_rows if r.get("pass")]
    top_rows = sorted(summary_rows, key=lambda row: (pd.notna(row["ci_low"]), row["ci_low"]), reverse=True)[:5]

    lines = [
        "# MLB CLV Ranking Diagnostics",
        "",
        f"Input CLV matches: {clv_matches_path}",
        f"Candidate edges: {candidate_path or 'not provided'}",
        f"CLV match rows: {max((r.get('n', 0) for r in summary_rows), default=0)}",
        f"Scores analyzed: {n_rows}",
        "",
        "## Ranking summary",
        "",
        "score_name | n_scored | spearman | ci_low | ci_high | n_blocks | monotonic_bins | top_minus_bottom_clv | pass",
        "|---|---:|---:|---:|---:|---:|---|---:|---|",
    ]

    for row in top_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["score_name"]),
                    str(row["n_scored"]),
                    f"{row['spearman']:+.4f}" if pd.notna(row["spearman"]) else "",
                    f"{row['ci_low']:+.4f}" if pd.notna(row["ci_low"]) else "",
                    f"{row['ci_high']:+.4f}" if pd.notna(row["ci_high"]) else "",
                    str(row["n_blocks"]),
                    str(bool(row["monotonic_bins"])),
                    f"{row['top_minus_bottom_clv']:+.4f}" if pd.notna(row["top_minus_bottom_clv"]) else "",
                    "yes" if row["pass"] else "no",
                ]
            )
            + " |"
        )

    if passed_rows:
        rec = "candidate_market_agreement_score_promising"
    else:
        rec = "no_ranker_found_flat_only"
        if summary_rows and all(r["n_scored"] < 1 for r in summary_rows):
            rec = "candidate_ranker_underpowered_collect_more_data"

    lines.extend(
        [
            "",
            "## Recommendation",
            f"Recommendation code: {rec}",
            "",
            "Notes:",
            "- Pass rule: n_scored >= min_n, ci_low > 0, top-minus-bottom CLV > 0, monotonic bins true.",
            "- This script does not promote Kelly sizing or production policy.",
            "- Interpret score diagnostics as post-hoc ranking evidence before any sizing changes.",
        ]
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    clv_path = Path(args.clv_matches_csv)
    if not clv_path.exists():
        raise FileNotFoundError(f"Missing required file: {clv_path}")

    df = load_csv(clv_path)
    if REQUIRED_SCORING_TARGET not in df.columns:
        raise ValueError(f"{clv_path} missing required '{REQUIRED_SCORING_TARGET}'")

    base = build_score_registry(df, candidate_scores=load_candidate_features(df, args.candidate_edges_csv))
    analysis_df = df.copy()
    alpha_columns = [
        "reference_prob",
        "execution_prob",
        "model_alpha",
        "execution_alpha",
        "total_edge_decomposed",
        "alpha_reconstruction_error",
    ]
    for name in alpha_columns:
        if name in base:
            analysis_df[name] = base[name]
    score_map = filter_scores_for_set(base, args.score_set)

    # Drop scores that are completely missing
    score_map = {
        name: values
        for name, values in score_map.items()
        if isinstance(values, pd.Series) and values.notna().any()
    }
    if not score_map:
        raise ValueError("No usable score columns were available in the provided CLV artifact.")

    summary_rows = [
        summarize_score(
            df,
            name,
            values,
            bootstrap_samples=args.bootstrap_samples,
            ci_level=args.ci_level,
            min_n=args.min_n,
            random_seed=args.random_seed,
        )
        for name, values in score_map.items()
    ]

    # Optional requested bin diagnostics
    bins_rows: list[dict[str, Any]] = []
    for name, values in score_map.items():
        bins = _score_bins_for_dataframe(
            df=df,
            score_name=name,
            score_values=values,
            n_bootstrap=args.bootstrap_samples,
            ci_level=args.ci_level,
            random_seed=args.random_seed,
        )
        if bins:
            bins_rows.extend(bins)

    # Slice diagnostics are intentionally limited to the most promising score names so
    # post-audit runs stay fast on large CLV artifacts and many bookmaker slices.
    top_slice_names = [
        row["score_name"]
        for row in sorted(
            summary_rows,
            key=lambda row: (
                bool(pd.notna(row.get("ci_low"))),
                row.get("ci_low") if pd.notna(row.get("ci_low")) else float("-inf"),
                row.get("spearman") if pd.notna(row.get("spearman")) else float("-inf"),
            ),
            reverse=True,
        )[:3]
    ]
    slice_score_map = {name: score_map[name] for name in top_slice_names if name in score_map}
    slice_summary = build_slice_frames(
        df,
        slice_score_map,
        bootstrap_samples=args.bootstrap_samples,
        ci_level=args.ci_level,
        min_n=args.min_n,
        random_seed=args.random_seed,
    )
    alpha_2d_rows = build_alpha_2d_buckets(
        analysis_df,
        bootstrap_samples=args.bootstrap_samples,
        ci_level=args.ci_level,
        random_seed=args.random_seed,
    )
    filter_replay_rows = build_filter_replay(
        analysis_df,
        bootstrap_samples=args.bootstrap_samples,
        ci_level=args.ci_level,
        random_seed=args.random_seed,
    )
    scorecard_rows = build_scorecard_rows(summary_rows)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "ranking_score_summary.csv"
    bins_path = output_dir / "ranking_score_bins.csv"
    slice_path = output_dir / "ranking_score_slice_summary.csv"
    scorecard_path = output_dir / "ranking_scorecard_summary.csv"
    alpha_rows_path = output_dir / "edge_decomposition_rows.csv"
    alpha_2d_path = output_dir / "edge_decomposition_2d_buckets.csv"
    filter_replay_path = output_dir / "edge_decomposition_filter_replay.csv"
    md_path = output_dir / "ranking_score_recommendation.md"

    summary_columns = [
        "score_name",
        "n",
        "n_scored",
        "spearman",
        "ci_low",
        "ci_high",
        "n_blocks",
        "monotonic_bins",
        "top_decile_mean_clv",
        "bottom_decile_mean_clv",
        "top_minus_bottom_clv",
        "pass",
        "bootstrap_method",
    ]
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False, columns=summary_columns)

    bins_columns = [
        "score_name",
        "bucket",
        "bucket_n",
        "mean_score",
        "min_score",
        "max_score",
        "mean_clv",
        "clv_ci_low",
        "clv_ci_high",
        "n_blocks",
        "same_book_share",
        "top_bookmaker_share",
    ]
    pd.DataFrame(bins_rows).to_csv(bins_path, index=False, columns=bins_columns)

    slice_columns = [
        "score_name",
        "slice",
        "n",
        "n_scored",
        "spearman",
        "ci_low",
        "ci_high",
        "n_blocks",
        "monotonic_bins",
        "top_decile_mean_clv",
        "bottom_decile_mean_clv",
        "top_minus_bottom_clv",
        "underpowered",
        "pass",
    ]
    pd.DataFrame(slice_summary).to_csv(slice_path, index=False, columns=slice_columns)

    scorecard_columns = ["score_class", *summary_columns]
    pd.DataFrame(scorecard_rows).to_csv(scorecard_path, index=False, columns=scorecard_columns)

    alpha_row_columns = [
        col
        for col in [
            "bet_id",
            "game_id",
            "game_date",
            "player_id",
            "bookmaker_at_bet",
            "bookmaker",
            "model_prob",
            "edge",
            "reference_prob",
            "execution_prob",
            "model_alpha",
            "execution_alpha",
            "total_edge_decomposed",
            "alpha_reconstruction_error",
            "clv_implied_prob",
            "profit",
            "stake",
        ]
        if col in analysis_df.columns
    ]
    if alpha_row_columns:
        analysis_df[alpha_row_columns].to_csv(alpha_rows_path, index=False)
    else:
        pd.DataFrame().to_csv(alpha_rows_path, index=False)

    alpha_2d_columns = [
        "model_alpha_bucket",
        "execution_alpha_bucket",
        "bucket_n",
        "n",
        "mean_model_alpha",
        "mean_execution_alpha",
        "profit",
        "staked",
        "roi",
        "max_drawdown",
        "mean_clv",
        "clv_ci_low",
        "clv_ci_high",
        "n_blocks",
    ]
    pd.DataFrame(alpha_2d_rows, columns=alpha_2d_columns).to_csv(alpha_2d_path, index=False)

    filter_replay_columns = [
        "filter_rule",
        "n",
        "mean_model_alpha",
        "mean_execution_alpha",
        "profit",
        "staked",
        "roi",
        "max_drawdown",
        "mean_clv",
        "clv_ci_low",
        "clv_ci_high",
        "n_blocks",
    ]
    pd.DataFrame(filter_replay_rows, columns=filter_replay_columns).to_csv(filter_replay_path, index=False)

    build_markdown(md_path, clv_path, summary_rows, args.candidate_edges_csv)

    return {
        "summary_csv": str(summary_path),
        "bins_csv": str(bins_path),
        "slice_csv": str(slice_path),
        "scorecard_csv": str(scorecard_path),
        "edge_decomposition_rows_csv": str(alpha_rows_path),
        "edge_decomposition_2d_buckets_csv": str(alpha_2d_path),
        "edge_decomposition_filter_replay_csv": str(filter_replay_path),
        "recommendation_md": str(md_path),
        "n_scores": len(score_map),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post-hoc MLB CLV ranking diagnostics.")
    parser.add_argument("--clv-matches-csv", required=True, help="Path to clv_matches.csv")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--candidate-edges-csv", default=None, help="Optional candidate edge artifact CSV")
    parser.add_argument("--score-set", choices=("default", "all"), default="default", help="Which score set to evaluate")
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--ci-level", type=float, default=0.95)
    parser.add_argument("--min-n", dest="min_n", type=int, default=100)
    parser.add_argument("--random-seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run(args)
    print("wrote")
    for key, path in (
        ("summary", result["summary_csv"]),
        ("bins", result["bins_csv"]),
        ("slice", result["slice_csv"]),
        ("scorecard", result["scorecard_csv"]),
        ("edge_decomposition_rows", result["edge_decomposition_rows_csv"]),
        ("edge_decomposition_2d", result["edge_decomposition_2d_buckets_csv"]),
        ("edge_decomposition_filter_replay", result["edge_decomposition_filter_replay_csv"]),
        ("recommendation", result["recommendation_md"]),
    ):
        print(f"{key}: {path}")


if __name__ == "__main__":
    main()
