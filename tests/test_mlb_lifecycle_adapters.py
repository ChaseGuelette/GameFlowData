from __future__ import annotations

from pathlib import Path

from scripts.run_mlb_quote_clean_audit_suite import build_arg_parser as build_audit_parser
from src.backtesting.mlb.sweep_config import build_arg_parser as build_sweep_parser
from src.models.mlb.lifecycle.adapters import (
    build_audit_command,
    build_ranker_command,
    build_sweep_command,
    build_training_command,
)
from src.models.mlb.lifecycle.config import resolve_lifecycle_config
from src.models.mlb.mlb_batter_train_pipeline import build_arg_parser as build_batter_parser
from src.models.mlb.mlb_train_pipeline import build_arg_parser as build_pitcher_parser

ROOT = Path(__file__).resolve().parents[1]


def _resolved(name: str):
    return resolve_lifecycle_config(ROOT / name)


def test_batter_profiles_share_adapter_without_stat_branches() -> None:
    hits = _resolved("configs/mlb/batter_hits/platoon_contact_independent.yaml")
    rbis = _resolved("configs/mlb/batter_rbis/baseline_independent.yaml")
    hits_command = build_training_command(hits.profile_obj, hits)
    rbi_command = build_training_command(rbis.profile_obj, rbis)
    assert "mlb_batter_train_pipeline.py" in hits_command[1]
    assert hits_command[hits_command.index("--stat") + 1] == "hits"
    assert rbi_command[rbi_command.index("--stat") + 1] == "rbis"
    assert "--exclude-prop-line" in rbi_command


def test_pitcher_uses_quantile_adapter() -> None:
    resolved = _resolved("configs/mlb/pitcher_strikeouts/baseline_independent.yaml")
    with_line_command = build_training_command(resolved.profile_obj, resolved)
    assert "mlb_train_pipeline.py" in with_line_command[1]
    assert "--stat" not in with_line_command
    assert "--copula" not in with_line_command
    assert "--force-exclude-features" not in with_line_command

    resolved.model.base = "no_prop_line"
    no_line_command = build_training_command(resolved.profile_obj, resolved)
    excluded = no_line_command[no_line_command.index("--force-exclude-features") + 1 :]
    assert "prop_line_pitcher_strikeouts" in excluded


def test_sweep_command_propagates_yaml_bl_bounds(tmp_path: Path) -> None:
    resolved = _resolved("configs/mlb/examples/start_from_scratch.yaml")
    resolved.evaluation.z_max = [0.25, 0.5]
    resolved.evaluation.max_weight = [0.4, 0.5]

    sweep = build_sweep_command(
        resolved, artifact_dir=tmp_path / "model", output_dir=tmp_path / "sweep"
    )

    assert sweep[sweep.index("--z-max") + 1 : sweep.index("--max-weight")] == ["0.25", "0.5"]
    assert sweep[sweep.index("--max-weight") + 1 : sweep.index("--edge")] == ["0.4", "0.5"]


def test_sweep_and_audit_commands_match_existing_clis(tmp_path: Path) -> None:
    resolved = _resolved("configs/mlb/batter_rbis/baseline_independent.yaml")
    sweep = build_sweep_command(resolved, artifact_dir=tmp_path / "model", output_dir=tmp_path / "sweep")
    assert "--bootstrap-samples" not in sweep
    assert "--no-prop-line" not in sweep
    assert sweep.count("--edge") == 1
    assert "--quote-clean" in sweep
    audit = build_audit_command(
        resolved,
        sweep_output_dir=tmp_path / "sweep",
        output_dir=tmp_path / "audit",
        artifact_dir=tmp_path / "model",
        bets_csvs=[tmp_path / "sweep/config_01/bets.csv"],
    )
    assert "--skip-dropout-audit" in audit
    assert "--bets-csv" in audit
    assert audit[audit.index("--snapshots-table") + 1] == "mlb_player_props_clv_snapshots"
    assert "--book-routing-policy" not in audit


def test_ranker_command_routes_exact_candidate_edges_and_all_scores(tmp_path: Path) -> None:
    candidate_edges = tmp_path / "sweep" / "config_02" / "bookmaker_candidate_edges.csv"
    command = build_ranker_command(
        clv_matches_csv=tmp_path / "audit" / "clv" / "config_02" / "clv_matches.csv",
        candidate_edges_csv=candidate_edges,
        output_dir=tmp_path / "ranker",
        bootstrap_samples=250,
        minimum_bets=100,
    )
    assert "--stat" not in command
    assert command[command.index("--bootstrap-samples") + 1] == "250"
    assert command[command.index("--score-set") + 1] == "all"
    assert Path(command[command.index("--candidate-edges-csv") + 1]) == candidate_edges


def test_generated_commands_parse_with_real_execution_clis(tmp_path: Path) -> None:
    pitcher = _resolved("configs/mlb/pitcher_strikeouts/baseline_independent.yaml")
    rbis = _resolved("configs/mlb/batter_rbis/baseline_independent.yaml")
    build_pitcher_parser().parse_args(build_training_command(pitcher.profile_obj, pitcher)[2:])
    build_batter_parser().parse_args(build_training_command(rbis.profile_obj, rbis)[2:])
    build_sweep_parser().parse_args(
        build_sweep_command(rbis, artifact_dir=tmp_path / "model", output_dir=tmp_path / "sweep")[2:]
    )
    build_audit_parser().parse_args(
        build_audit_command(
            rbis,
            sweep_output_dir=tmp_path / "sweep",
            output_dir=tmp_path / "audit",
            artifact_dir=tmp_path / "model",
        )[2:]
    )
