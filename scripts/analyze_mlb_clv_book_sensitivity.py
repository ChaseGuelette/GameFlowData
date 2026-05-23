#!/usr/bin/env python3
"""Bookmaker deconcentration / sensitivity report for saved MLB CLV matches.

This is a post-CLV-analysis script. It reads `clv_matches.csv` files already
produced by `scripts/analyze_mlb_batter_hits_clv.py` or the quote-clean audit
suite, then recomputes the same mean-CLV and edge-ranking gates under fixed
book filters such as ESPNBet-only, excluding ESPNBet, and excluding ProphetX.

It does not query the database, retrain models, rerun backtests, or change bet
selection. Treat the output as a sensitivity report on an already-generated
quote-clean CLV artifact.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.analyze_mlb_batter_hits_clv import summarize_group  # noqa: E402

DEFAULT_BOOKS = ("espnbet", "prophetx")


def _fmt_pct(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return ""
    return f"{float(value):.2%}"


def _fmt_float(value: float | None, digits: int = 6) -> str:
    if value is None or not math.isfinite(float(value)):
        return ""
    return f"{float(value):+.{digits}f}"


def _fmt_money(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return ""
    return f"${float(value):,.2f}"


def _clean_bookmaker(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower().replace(" ", "")


def _read_labels_from_suite_summary(path: Path) -> list[str]:
    """Parse config labels from the suite summary's validation table."""
    if not path.exists():
        return []
    labels: list[str] = []
    in_table = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("| Label | Gate |"):
            in_table = True
            continue
        if in_table and (not line.startswith("|") or line.startswith("## ")):
            break
        if not in_table or line.startswith("|---"):
            continue
        parts = [p.strip() for p in line.strip().strip("|").split("|")]
        if parts and parts[0] and parts[0] != "Label":
            labels.append(parts[0])
    return labels


def discover_match_files(audit_suite_dir: Path, labels: Iterable[str] | None = None) -> list[Path]:
    clv_root = audit_suite_dir / "clv"
    if not clv_root.exists():
        raise FileNotFoundError(f"No clv/ directory found under {audit_suite_dir}")
    wanted = set(labels or [])
    paths = sorted(clv_root.glob("*/clv_matches.csv"))
    if wanted:
        paths = [p for p in paths if p.parent.name in wanted]
    if not paths:
        label_msg = f" for labels {sorted(wanted)}" if wanted else ""
        raise FileNotFoundError(f"No clv_matches.csv files found{label_msg} under {clv_root}")
    return paths


def normalize_matches(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    book_col = "bookmaker_at_bet" if "bookmaker_at_bet" in out.columns else "bookmaker"
    if book_col not in out.columns:
        raise ValueError("clv_matches.csv must contain bookmaker_at_bet or bookmaker")
    out["bookmaker_key"] = out[book_col].map(_clean_bookmaker)
    if "odds_at_bet" not in out.columns and "odds" in out.columns:
        out["odds_at_bet"] = pd.to_numeric(out["odds"], errors="coerce")
    else:
        out["odds_at_bet"] = pd.to_numeric(out.get("odds_at_bet"), errors="coerce")
    for col in ["profit", "stake", "edge", "clv_implied_prob"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _book_counts(df: pd.DataFrame) -> dict[str, int]:
    counts = df["bookmaker_key"].value_counts(dropna=False).to_dict()
    return {str(k): int(v) for k, v in counts.items() if str(k)}


def _numeric_sum(df: pd.DataFrame, column: str) -> float | None:
    if column not in df.columns:
        return None
    total = 0.0
    for value in df[column].tolist():
        try:
            if pd.notna(value):
                total += float(value)
        except Exception:
            continue
    return total


def _roi_metrics(df: pd.DataFrame) -> dict[str, float | None]:
    profit = _numeric_sum(df, "profit")
    stake = _numeric_sum(df, "stake")
    roi = profit / stake if profit is not None and stake and stake > 0 else None
    return {"profit": profit, "stake": stake, "roi": roi}


def summarize_slice(
    df: pd.DataFrame,
    *,
    config: str,
    slice_name: str,
    description: str,
    bootstrap_samples: int,
    ci_level: float,
    total_config_bets: int,
) -> dict:
    summary = summarize_group(df, slice_name, bootstrap_samples, ci_level)
    roi = _roi_metrics(df)
    counts = _book_counts(df)
    top_book = None
    top_book_share = None
    if counts:
        top_book, top_count = max(counts.items(), key=lambda kv: kv[1])
        top_book_share = top_count / len(df) if len(df) else None
    espn_count = counts.get("espnbet", 0)
    prophetx_count = counts.get("prophetx", 0)
    row = {
        "config": config,
        "slice": slice_name,
        "description": description,
        "n_total_config_bets": total_config_bets,
        "n": summary.get("n"),
        "n_share_of_config": (summary.get("n") / total_config_bets) if total_config_bets else None,
        "n_scored": summary.get("n_scored"),
        "n_same_book": summary.get("n_same_book"),
        "n_consensus_fallback": summary.get("n_consensus_fallback"),
        "n_unmatched": summary.get("n_unmatched"),
        "mean_clv_implied_prob": summary.get("mean_clv_implied_prob"),
        "mean_clv_ci_low": summary.get("mean_clv_ci_low"),
        "mean_clv_ci_high": summary.get("mean_clv_ci_high"),
        "edge_clv_spearman": summary.get("edge_clv_spearman"),
        "edge_clv_ci_low": summary.get("edge_clv_ci_low"),
        "edge_clv_ci_high": summary.get("edge_clv_ci_high"),
        "n_blocks": summary.get("n_blocks"),
        "bootstrap_method": summary.get("bootstrap_method"),
        "profit": roi["profit"],
        "stake": roi["stake"],
        "roi": roi["roi"],
        "top_bookmaker": top_book,
        "top_bookmaker_share": top_book_share,
        "espnbet_bets": espn_count,
        "espnbet_share": (espn_count / len(df)) if len(df) else None,
        "prophetx_bets": prophetx_count,
        "prophetx_share": (prophetx_count / len(df)) if len(df) else None,
    }
    row["mean_clv_pass"] = bool(pd.notna(row["mean_clv_ci_low"]) and row["mean_clv_ci_low"] > 0)
    row["edge_ranking_pass"] = bool(pd.notna(row["edge_clv_ci_low"]) and row["edge_clv_ci_low"] > 0)
    return row


def build_slices(df: pd.DataFrame, books: Iterable[str]) -> list[tuple[str, str, pd.DataFrame]]:
    books = tuple(_clean_bookmaker(b) for b in books)
    plus_money = df.loc[df["odds_at_bet"] >= 100].copy()
    slices: list[tuple[str, str, pd.DataFrame]] = [
        ("overall", "All bets in config", df),
        ("plus_money_only", "Bets where odds_at_bet >= +100", plus_money),
    ]
    for book in books:
        book_only = df.loc[df["bookmaker_key"] == book].copy()
        exclude_book = df.loc[df["bookmaker_key"] != book].copy()
        plus_exclude_book = df.loc[(df["odds_at_bet"] >= 100) & (df["bookmaker_key"] != book)].copy()
        slices.extend(
            [
                (f"{book}_only", f"Only bets selected at {book}", book_only),
                (f"exclude_{book}", f"All bets except {book}", exclude_book),
                (
                    f"plus_money_exclude_{book}",
                    f"Plus-money bets except {book}",
                    plus_exclude_book,
                ),
            ]
        )
    if len(books) >= 2:
        excluded = list(books)
        exclude_primary = df.loc[~df["bookmaker_key"].isin(excluded)].copy()
        plus_exclude_primary = df.loc[(df["odds_at_bet"] >= 100) & (~df["bookmaker_key"].isin(excluded))].copy()
        slices.append((
            "exclude_primary_books",
            f"All bets excluding {', '.join(books)}",
            exclude_primary,
        ))
        slices.append((
            "plus_money_exclude_primary_books",
            f"Plus-money bets excluding {', '.join(books)}",
            plus_exclude_primary,
        ))
    return slices


def write_markdown(path: Path, rows: list[dict]) -> None:
    lines = [
        "# MLB CLV Book Sensitivity / Deconcentration Report",
        "",
        "This report is post-hoc on saved `clv_matches.csv` artifacts. It does not rerun selection, retrain models, or query the database.",
        "",
        "Interpretation rules:",
        "- Mean CLV pass means the slice's mean implied-prob CLV lower CI is > 0.",
        "- Edge-ranking pass means Spearman(edge, CLV) lower CI is > 0.",
        "- If mean CLV passes but edge-ranking fails, the slice can be a flat-paper candidate only; it does not justify Kelly/edge sizing.",
        "",
        "| Config | Slice | N | Scored | ROI | Mean CLV [CI] | Spearman [CI] | ESPNBet | ProphetX | Top Book | Mean CLV Pass | Edge Rank Pass |",
        "|---|---|---:|---:|---:|---|---|---:|---:|---|---|---|",
    ]
    for row in rows:
        mean = f"{_fmt_float(row.get('mean_clv_implied_prob'))} [{_fmt_float(row.get('mean_clv_ci_low'))}, {_fmt_float(row.get('mean_clv_ci_high'))}]"
        corr = f"{_fmt_float(row.get('edge_clv_spearman'))} [{_fmt_float(row.get('edge_clv_ci_low'))}, {_fmt_float(row.get('edge_clv_ci_high'))}]"
        top = row.get("top_bookmaker") or ""
        top_share = _fmt_pct(row.get("top_bookmaker_share"))
        if top and top_share:
            top = f"{top} {top_share}"
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("config", "")),
                    str(row.get("slice", "")),
                    str(row.get("n", "")),
                    str(row.get("n_scored", "")),
                    _fmt_pct(row.get("roi")),
                    mean,
                    corr,
                    _fmt_pct(row.get("espnbet_share")),
                    _fmt_pct(row.get("prophetx_share")),
                    top,
                    "yes" if row.get("mean_clv_pass") else "no",
                    "yes" if row.get("edge_ranking_pass") else "no",
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Relevant prior lessons/invariants",
            "",
            "- Positive mean CLV is not enough to validate edge/Kelly sizing when edge ranking fails.",
            "- Feature expansion remains blocked until quote-clean selection policy and book sensitivity are understood.",
            "- Empirical-CDF probabilities remain required; this script only reads saved CLV outputs.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict:
    suite_dir = Path(args.audit_suite_dir)
    labels = args.config or []
    if args.use_suite_selected and not labels:
        labels = _read_labels_from_suite_summary(suite_dir / "suite_summary.md")
    match_files = discover_match_files(suite_dir, labels or None)

    rows: list[dict] = []
    for path in match_files:
        config = path.parent.name
        df = normalize_matches(pd.read_csv(path))
        total_config_bets = len(df)
        for slice_name, description, slice_df in build_slices(df, args.books):
            slice_df = slice_df.copy()
            if len(slice_df) < args.min_bets:
                continue
            rows.append(
                summarize_slice(
                    slice_df,
                    config=config,
                    slice_name=slice_name,
                    description=description,
                    bootstrap_samples=args.bootstrap_samples,
                    ci_level=args.ci_level,
                    total_config_bets=total_config_bets,
                )
            )

    output_dir = Path(args.output_dir) if args.output_dir else suite_dir / "book_sensitivity"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "book_sensitivity_summary.csv"
    md_path = output_dir / "book_sensitivity_summary.md"
    if rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    else:
        csv_path.write_text("", encoding="utf-8")
    write_markdown(md_path, rows)
    return {"rows": len(rows), "csv": str(csv_path), "markdown": str(md_path)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze MLB CLV bookmaker sensitivity from saved clv_matches.csv files")
    parser.add_argument("--audit-suite-dir", required=True, help="Audit suite output dir containing clv/<config>/clv_matches.csv")
    parser.add_argument("--config", action="append", help="Specific config label to include. Repeatable. Defaults to all configs unless --use-suite-selected is used.")
    parser.add_argument("--use-suite-selected", action="store_true", help="Use labels from suite_summary.md validation table when --config is omitted")
    parser.add_argument("--books", nargs="+", default=list(DEFAULT_BOOKS), help="Bookmakers to test as only/exclude slices")
    parser.add_argument("--output-dir", default=None, help="Output dir. Defaults to <audit-suite-dir>/book_sensitivity")
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--ci-level", type=float, default=0.95)
    parser.add_argument("--min-bets", type=int, default=1, help="Skip slices with fewer than this many bets")
    return parser.parse_args()


if __name__ == "__main__":
    result = run(parse_args())
    print(f"Wrote {result['rows']} book-sensitivity rows")
    print(result["csv"])
    print(result["markdown"])
