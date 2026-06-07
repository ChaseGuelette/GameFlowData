from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PITCHER_PIPELINE = ROOT / "src" / "models" / "mlb" / "mlb_train_pipeline.py"
BATTER_PIPELINE = ROOT / "src" / "models" / "mlb" / "mlb_batter_train_pipeline.py"


def _source(path: Path) -> str:
    return path.read_text()


def test_pitcher_training_pipeline_uses_explicit_training_loader_request():
    source = _source(PITCHER_PIPELINE)
    assert "PitcherTrainingLoader" in source
    assert "TrainingFeatureRequest" in source
    assert "self.training_loader.load" in source
    assert "self.feature_store.get_training_dataset" not in source


def test_batter_training_pipeline_uses_explicit_training_loader_request():
    source = _source(BATTER_PIPELINE)
    assert "BatterTrainingLoader" in source
    assert "TrainingFeatureRequest" in source
    assert "self.training_loader.load" in source
    assert "self.feature_store.get_training_dataset" not in source
