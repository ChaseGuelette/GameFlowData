import logging

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_pinball_loss
from sklearn.model_selection import TimeSeriesSplit

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ImprovedFeatureSelector:
    """
    Robust feature selection pipeline for quantile regression.

    Supports per-quantile feature selection: each quantile gets its own
    feature ranking and optimal feature count.
    """

    def __init__(self, n_splits: int = 3, quantiles: list = None, tolerance: float = 0.005):
        self.n_splits = n_splits
        self.quantiles = quantiles or [0.10, 0.25, 0.50, 0.75, 0.90]
        self.tolerance = tolerance

    def select_features_per_quantile(
        self, df: pd.DataFrame, target_col: str, candidate_cols: list[str], model_name: str = "Model"
    ) -> dict[float, list[str]]:
        """
        Run feature selection independently for each quantile.

        Returns dict mapping quantile -> selected feature list.
        """
        logger.info(f"--- Per-Quantile Feature Selection for {model_name} ---")
        logger.info(f"Target: {target_col}, Candidates: {len(candidate_cols)}")

        # Preprocessing
        if "game_date" in df.columns:
            df = df.sort_values("game_date").reset_index(drop=True)

        valid_data = df.dropna(subset=[target_col] + candidate_cols).copy()

        if valid_data.empty:
            logger.warning(f"No valid data for {model_name}. Skipping.")
            return {q: [] for q in self.quantiles}

        X = valid_data[candidate_cols]
        y = valid_data[target_col]

        results = {}
        for q in self.quantiles:
            logger.info(f"\n  === Q{q:.2f} for {model_name} ===")

            # 1. Rank features at this quantile
            ranked = self._rank_features_for_quantile(X, y, q)
            logger.info(f"  Top 5: {ranked[:5]}")

            # 2. Optimize count for this quantile
            selected = self._optimize_count_for_quantile(X, y, ranked, q)
            logger.info(f"  Selected {len(selected)} features for Q{q:.2f}")

            results[q] = selected

        return results

    def _rank_features_for_quantile(self, X: pd.DataFrame, y: pd.Series, quantile: float) -> list[str]:
        """
        Rank features using Permutation Importance at a specific quantile level.
        """
        tscv = TimeSeriesSplit(n_splits=self.n_splits)
        train_index = []
        for train_index, _ in tscv.split(X):
            pass

        if len(train_index) == 0:
            split_idx = int(len(X) * 0.8)
            train_index = range(split_idx)

        X_train = X.iloc[train_index]
        y_train = y.iloc[train_index]

        # Train proxy model at the TARGET quantile
        model = xgb.XGBRegressor(
            objective="reg:quantileerror",
            quantile_alpha=quantile,
            n_estimators=100,
            learning_rate=0.05,
            max_depth=3,
            n_jobs=-1,
            random_state=42,
        )
        model.fit(X_train, y_train)

        # Quantile-specific scorer
        def quantile_scorer(estimator, X_eval, y_eval):
            preds = estimator.predict(X_eval)
            return -mean_pinball_loss(y_eval, preds, alpha=quantile)

        result = permutation_importance(
            model,
            X_train,
            y_train,
            n_repeats=5,
            random_state=42,
            n_jobs=-1,
            scoring=quantile_scorer,
        )

        importances = result.importances_mean
        feature_importance = pd.DataFrame({"feature": X.columns, "importance": importances})
        ranked = feature_importance.sort_values("importance", ascending=False)

        positive_features = ranked[ranked["importance"] > 0]["feature"].tolist()

        if len(positive_features) < 3:
            return ranked["feature"].tolist()

        return positive_features

    def _optimize_count_for_quantile(
        self, X: pd.DataFrame, y: pd.Series, ranked_candidates: list[str], quantile: float
    ) -> list[str]:
        """
        Find optimal K for a single quantile using its pinball loss via TimeSeriesSplit CV.
        """
        tscv = TimeSeriesSplit(n_splits=self.n_splits)

        step = 1
        max_feats = len(ranked_candidates)
        counts_to_test = list(range(5, max_feats + 1, step))
        if max_feats not in counts_to_test:
            counts_to_test.append(max_feats)
        if max_feats < 5:
            counts_to_test = [max_feats]

        best_score = float("inf")
        best_k = counts_to_test[0]
        splits = list(tscv.split(X))

        for k in counts_to_test:
            current_features = ranked_candidates[:k]
            cv_scores = []

            for train_idx, val_idx in splits:
                X_train = X.iloc[train_idx][current_features]
                X_val = X.iloc[val_idx][current_features]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

                model = xgb.XGBRegressor(
                    objective="reg:quantileerror",
                    quantile_alpha=quantile,
                    n_estimators=50,
                    max_depth=3,
                    learning_rate=0.1,
                    n_jobs=-1,
                    random_state=42,
                )
                model.fit(X_train, y_train)
                preds = model.predict(X_val)
                loss = mean_pinball_loss(y_val, preds, alpha=quantile)
                cv_scores.append(loss)

            avg_score = np.mean(cv_scores)

            if avg_score < best_score:
                best_score = avg_score
                best_k = k

        # Prefer more features when loss is within tolerance of the best.
        # This allows matchup/contextual features that don't improve raw
        # pinball loss but may carry useful signal for betting edges.
        if self.tolerance > 0:
            threshold = best_score * (1 + self.tolerance)
            for k in counts_to_test:
                if k <= best_k:
                    continue
                current_features = ranked_candidates[:k]
                cv_scores = []
                for train_idx, val_idx in splits:
                    X_train = X.iloc[train_idx][current_features]
                    X_val = X.iloc[val_idx][current_features]
                    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
                    model = xgb.XGBRegressor(
                        objective="reg:quantileerror",
                        quantile_alpha=quantile,
                        n_estimators=50,
                        max_depth=3,
                        learning_rate=0.1,
                        n_jobs=-1,
                        random_state=42,
                    )
                    model.fit(X_train, y_train)
                    preds = model.predict(X_val)
                    loss = mean_pinball_loss(y_val, preds, alpha=quantile)
                    cv_scores.append(loss)
                avg_score = np.mean(cv_scores)
                if avg_score <= threshold:
                    best_k = k

        logger.info(f"    Optimal K={best_k} (Pinball Loss: {best_score:.4f}, tolerance={self.tolerance:.3f})")
        return ranked_candidates[:best_k]


def get_candidate_columns(df: pd.DataFrame, target_col: str) -> list[str]:
    """
    Identify potential feature columns from the dataframe.
    """
    excluded_suffixes = ["_id", "_date", "_name", "season_id", "game_id", "team_id", "opponent_id", "player_id"]
    excluded_exact = [
        "actual_minutes",
        "actual_pts",
        "actual_reb",
        "actual_ast",
        "actual_threes",
        "pts_per_min",
        "reb_per_min",
        "ast_per_min",
        "threes_per_min",
        "position_group",
        # Removed "is_home" so it can be selected
    ]

    candidates = []
    for col in df.columns:
        if col == target_col:
            continue
        if col in excluded_exact:
            continue
        if any(col.endswith(s) for s in excluded_suffixes):
            continue
        if df[col].dtype == object:
            continue

        candidates.append(col)

    return candidates


# Alias for compatibility with existing imports
FeatureSelector = ImprovedFeatureSelector
