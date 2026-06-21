from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run_mlb_stat_ablation.ps1"
PITCHER_WRAPPER = ROOT / "scripts" / "run_pitcher_k_ablation.ps1"
RESUME_RUNNER = ROOT / "scripts" / "resume_mlb_stat_ablation_audit.ps1"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_generic_runner_has_batter_and_pitcher_profiles() -> None:
    text = read(RUNNER)

    assert "batter_hits" in text
    assert "pitcher_strikeouts" in text
    assert "src\\models\\mlb\\mlb_batter_train_pipeline.py" in text
    assert "src\\models\\mlb\\mlb_train_pipeline.py" in text
    assert "--stats', 'batter_hits" in text
    assert "--stats', 'pitcher_strikeouts" in text


def test_generic_runner_threads_quote_clean_dense_clv_defaults() -> None:
    text = read(RUNNER)

    assert "--quote-clean" in text
    assert "--quote-decision-policy', 'slate_or_tminus" in text
    assert "--line-source', 'mlb_player_props_clv_snapshots" in text
    assert "--snapshots-table', 'mlb_player_props_clv_snapshots" in text
    assert "--book-routing-policy', 'preferred_book_first" in text
    assert "--direction', $profileConfig.DefaultDirection" in text
    assert "DefaultDirection = 'under'" in text


def test_generic_runner_dry_run_prints_all_lifecycle_commands_without_execution() -> None:
    text = read(RUNNER)

    assert "[1/4] Train" in text
    assert "[2/4] Sweep" in text
    assert "[3/4] Audit" in text
    assert "[4/4] Ranker" in text
    assert "$DryRun" in text
    assert "if ($DryRun)" in text
    assert "SkipTrain" in text
    assert "SkipSweep" in text
    assert "SkipAudit" in text


def test_pitcher_wrapper_delegates_to_generic_runner() -> None:
    text = read(PITCHER_WRAPPER)

    assert "run_mlb_stat_ablation.ps1" in text
    assert "-Profile" in text
    assert "pitcher_strikeouts" in text
    assert "Variant = $Variant" in text
    assert "DryRun = $true" in text
    assert "mlb_train_pipeline.py" not in text


def test_resume_runner_is_profile_generic_and_uses_generic_audit_suite() -> None:
    text = read(RESUME_RUNNER)

    assert "[ValidateSet('batter_hits','pitcher_strikeouts')]" in text
    assert "Get-ProfileConfig" in text
    assert "run_mlb_quote_clean_audit_suite.py" in text
    assert "analyze_mlb_clv_ranking_diagnostics.py" in text
    assert "--stats', $profileConfig.SweepStat" in text
    assert "pitcher_strikeouts" in text
    assert "ArtifactFilter" in text
