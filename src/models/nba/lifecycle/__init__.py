"""Report-only NBA lifecycle. Runtime dependencies are imported at execution seams."""

STAGES = ("validate", "train_or_attach", "artifact_identity", "sweep", "audit", "ranker", "decision")
