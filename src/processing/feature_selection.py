import logging

import numpy as np
import pandas as pd
import xgboost as xgb
from scipy.stats import binom, nbinom
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

    # ------------------------------------------------------------------
    # NLL-based feature selection (for NegBin distributional models)
    # ------------------------------------------------------------------

    def select_features_nll(
        self, df: pd.DataFrame, target_col: str, candidate_cols: list[str], model_name: str = "Model"
    ) -> list[str]:
        """NLL-based feature selection for NegBin count models.

        Uses a Poisson-objective XGBoost as a fast mu-proxy, then scores
        features by their contribution to NB NLL reduction.

        Returns a single feature list (not per-quantile).
        """
        logger.info(f"--- NLL-based Feature Selection for {model_name} ---")
        logger.info(f"Target: {target_col}, Candidates: {len(candidate_cols)}")

        if "game_date" in df.columns:
            df = df.sort_values("game_date").reset_index(drop=True)

        valid_data = df.dropna(subset=[target_col] + candidate_cols).copy()

        if valid_data.empty:
            logger.warning(f"No valid data for {model_name}. Skipping.")
            return []

        X = valid_data[candidate_cols]
        y = valid_data[target_col]

        # 1. Rank features by NLL contribution
        ranked = self._rank_features_nll(X, y)
        logger.info(f"  Top 10 by NLL: {ranked[:10]}")

        # 2. Optimize feature count via CV
        selected = self._optimize_count_nll(X, y, ranked)
        logger.info(f"  Selected {len(selected)} features for {model_name}")

        return selected

    def _rank_features_nll(self, X: pd.DataFrame, y: pd.Series) -> list[str]:
        """Rank features using permutation importance with NB NLL scorer.

        Trains a Poisson-objective XGBoost as a fast mu-proxy, then
        estimates alpha from residual variance and scores by NB NLL.
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

        # Train Poisson proxy model (fast, predicts mu)
        model = xgb.XGBRegressor(
            objective="count:poisson",
            n_estimators=100,
            learning_rate=0.05,
            max_depth=3,
            n_jobs=-1,
            random_state=42,
        )
        model.fit(X_train, y_train)

        # NB NLL scorer: use predicted mu + moment-estimated alpha
        def nll_scorer(estimator, X_eval, y_eval):
            mu_pred = np.clip(estimator.predict(X_eval), 1e-4, None)
            y_np = y_eval.values if hasattr(y_eval, "values") else np.asarray(y_eval)

            # Moment-method alpha estimate: alpha = (var/mu - 1) / mu
            residuals = y_np - mu_pred
            var_est = np.mean(residuals ** 2)
            mu_mean = np.mean(mu_pred)
            alpha_est = max(0.01, (var_est / mu_mean - 1) / max(mu_mean, 1e-4))

            # Compute NB NLL: parameterize as (n=1/alpha, p=n/(n+mu))
            n = 1.0 / alpha_est
            p = n / (n + mu_pred)
            p = np.clip(p, 1e-10, 1 - 1e-10)

            log_probs = nbinom.logpmf(y_np.astype(int), n, p)
            log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
            return float(np.mean(log_probs))  # higher = better

        result = permutation_importance(
            model,
            X_train,
            y_train,
            n_repeats=5,
            random_state=42,
            n_jobs=-1,
            scoring=nll_scorer,
        )

        importances = result.importances_mean
        feature_importance = pd.DataFrame({"feature": X.columns, "importance": importances})
        ranked = feature_importance.sort_values("importance", ascending=False)

        positive_features = ranked[ranked["importance"] > 0]["feature"].tolist()

        if len(positive_features) < 3:
            return ranked["feature"].tolist()

        return positive_features

    def _optimize_count_nll(
        self, X: pd.DataFrame, y: pd.Series, ranked_candidates: list[str]
    ) -> list[str]:
        """Find optimal K using NB NLL via TimeSeriesSplit CV."""
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
                y_train_fold = y.iloc[train_idx]
                y_val_fold = y.iloc[val_idx]

                model = xgb.XGBRegressor(
                    objective="count:poisson",
                    n_estimators=50,
                    max_depth=3,
                    learning_rate=0.1,
                    n_jobs=-1,
                    random_state=42,
                )
                model.fit(X_train, y_train_fold)
                mu_pred = np.clip(model.predict(X_val), 1e-4, None)
                y_val_np = y_val_fold.values if hasattr(y_val_fold, "values") else np.asarray(y_val_fold)

                # Moment-method alpha from this fold
                residuals = y_val_np - mu_pred
                var_est = np.mean(residuals ** 2)
                mu_mean = np.mean(mu_pred)
                alpha_est = max(0.01, (var_est / mu_mean - 1) / max(mu_mean, 1e-4))

                n = 1.0 / alpha_est
                p = n / (n + mu_pred)
                p = np.clip(p, 1e-10, 1 - 1e-10)

                log_probs = nbinom.logpmf(y_val_np.astype(int), n, p)
                log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
                nll = -float(np.mean(log_probs))
                cv_scores.append(nll)

            avg_score = np.mean(cv_scores)

            if avg_score < best_score:
                best_score = avg_score
                best_k = k

        # Prefer more features within tolerance
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
                    y_train_fold = y.iloc[train_idx]
                    y_val_fold = y.iloc[val_idx]
                    model = xgb.XGBRegressor(
                        objective="count:poisson",
                        n_estimators=50,
                        max_depth=3,
                        learning_rate=0.1,
                        n_jobs=-1,
                        random_state=42,
                    )
                    model.fit(X_train, y_train_fold)
                    mu_pred = np.clip(model.predict(X_val), 1e-4, None)
                    y_val_np = y_val_fold.values if hasattr(y_val_fold, "values") else np.asarray(y_val_fold)

                    residuals = y_val_np - mu_pred
                    var_est = np.mean(residuals ** 2)
                    mu_mean = np.mean(mu_pred)
                    alpha_est = max(0.01, (var_est / mu_mean - 1) / max(mu_mean, 1e-4))

                    n = 1.0 / alpha_est
                    p = n / (n + mu_pred)
                    p = np.clip(p, 1e-10, 1 - 1e-10)

                    log_probs = nbinom.logpmf(y_val_np.astype(int), n, p)
                    log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
                    nll = -float(np.mean(log_probs))
                    cv_scores.append(nll)
                avg_score = np.mean(cv_scores)
                if avg_score <= threshold:
                    best_k = k

        logger.info(f"    Optimal K={best_k} (NB NLL: {best_score:.4f}, tolerance={self.tolerance:.3f})")
        return ranked_candidates[:best_k]

    # ------------------------------------------------------------------
    # Binomial NLL-based feature selection (for Binomial count models)
    # ------------------------------------------------------------------

    def select_features_binomial_nll(
        self,
        df: pd.DataFrame,
        target_col: str,
        candidate_cols: list[str],
        at_bats_col: str = "at_bats",
        model_name: str = "Model",
    ) -> list[str]:
        """Binomial NLL-based feature selection for hits models.

        Uses a Poisson-objective XGBoost as a fast mu-proxy, then scores
        features by their contribution to binomial NLL reduction.

        Args:
            df: DataFrame with features, target, and at_bats columns.
            target_col: Name of the target column (hits).
            candidate_cols: Feature column names to consider.
            at_bats_col: Column with at-bats values.
            model_name: Display name for logging.

        Returns a single feature list.
        """
        logger.info(f"--- Binomial NLL Feature Selection for {model_name} ---")
        logger.info(f"Target: {target_col}, At-bats: {at_bats_col}, Candidates: {len(candidate_cols)}")

        if "game_date" in df.columns:
            df = df.sort_values("game_date").reset_index(drop=True)

        valid_data = df.dropna(subset=[target_col] + candidate_cols).copy()

        if valid_data.empty:
            logger.warning(f"No valid data for {model_name}. Skipping.")
            return []

        X = valid_data[candidate_cols]
        y = valid_data[target_col]
        at_bats = valid_data[at_bats_col].values if at_bats_col in valid_data.columns else np.full(len(y), 3.5)

        # 1. Rank features by binomial NLL contribution
        ranked = self._rank_features_binomial_nll(X, y, at_bats)
        logger.info(f"  Top 10 by binomial NLL: {ranked[:10]}")

        # 2. Optimize feature count via CV
        selected = self._optimize_count_binomial_nll(X, y, at_bats, ranked)
        logger.info(f"  Selected {len(selected)} features for {model_name}")

        return selected

    def _rank_features_binomial_nll(
        self, X: pd.DataFrame, y: pd.Series, at_bats: np.ndarray
    ) -> list[str]:
        """Rank features using permutation importance with binomial NLL scorer.

        Trains a Poisson-objective XGBoost as a fast mu-proxy, then
        estimates p = mu / at_bats and scores by binomial NLL.
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

        # Train Poisson proxy model (fast, predicts mu ≈ n*p)
        model = xgb.XGBRegressor(
            objective="count:poisson",
            n_estimators=100,
            learning_rate=0.05,
            max_depth=3,
            n_jobs=-1,
            random_state=42,
        )
        model.fit(X_train, y_train)

        # Binomial NLL scorer
        def binom_nll_scorer(estimator, X_eval, y_eval):
            mu_pred = np.clip(estimator.predict(X_eval), 1e-4, None)
            y_np = y_eval.values if hasattr(y_eval, "values") else np.asarray(y_eval)

            # Get at_bats for the eval indices
            eval_idx = X_eval.index if hasattr(X_eval, "index") else range(len(X_eval))
            # Map back to original indices
            ab_eval = np.array([at_bats[i] for i in eval_idx])
            ab_eval = np.maximum(ab_eval, 1.0)

            # Estimate p from Poisson mu
            p_est = np.clip(mu_pred / ab_eval, 1e-7, 1 - 1e-7)

            log_probs = binom.logpmf(y_np.astype(int), ab_eval.astype(int), p_est)
            log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
            return float(np.mean(log_probs))  # higher = better

        result = permutation_importance(
            model,
            X_train,
            y_train,
            n_repeats=5,
            random_state=42,
            n_jobs=-1,
            scoring=binom_nll_scorer,
        )

        importances = result.importances_mean
        feature_importance = pd.DataFrame({"feature": X.columns, "importance": importances})
        ranked = feature_importance.sort_values("importance", ascending=False)

        positive_features = ranked[ranked["importance"] > 0]["feature"].tolist()

        if len(positive_features) < 3:
            return ranked["feature"].tolist()

        return positive_features

    def _optimize_count_binomial_nll(
        self, X: pd.DataFrame, y: pd.Series, at_bats: np.ndarray, ranked_candidates: list[str]
    ) -> list[str]:
        """Find optimal K using binomial NLL via TimeSeriesSplit CV."""
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
                y_train_fold = y.iloc[train_idx]
                y_val_fold = y.iloc[val_idx]
                ab_val = np.maximum(at_bats[val_idx], 1.0)

                model = xgb.XGBRegressor(
                    objective="count:poisson",
                    n_estimators=50,
                    max_depth=3,
                    learning_rate=0.1,
                    n_jobs=-1,
                    random_state=42,
                )
                model.fit(X_train, y_train_fold)
                mu_pred = np.clip(model.predict(X_val), 1e-4, None)
                y_val_np = y_val_fold.values if hasattr(y_val_fold, "values") else np.asarray(y_val_fold)

                p_est = np.clip(mu_pred / ab_val, 1e-7, 1 - 1e-7)

                log_probs = binom.logpmf(y_val_np.astype(int), ab_val.astype(int), p_est)
                log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
                nll = -float(np.mean(log_probs))
                cv_scores.append(nll)

            avg_score = np.mean(cv_scores)

            if avg_score < best_score:
                best_score = avg_score
                best_k = k

        # Prefer more features within tolerance
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
                    y_train_fold = y.iloc[train_idx]
                    y_val_fold = y.iloc[val_idx]
                    ab_val = np.maximum(at_bats[val_idx], 1.0)

                    model = xgb.XGBRegressor(
                        objective="count:poisson",
                        n_estimators=50,
                        max_depth=3,
                        learning_rate=0.1,
                        n_jobs=-1,
                        random_state=42,
                    )
                    model.fit(X_train, y_train_fold)
                    mu_pred = np.clip(model.predict(X_val), 1e-4, None)
                    y_val_np = y_val_fold.values if hasattr(y_val_fold, "values") else np.asarray(y_val_fold)

                    p_est = np.clip(mu_pred / ab_val, 1e-7, 1 - 1e-7)

                    log_probs = binom.logpmf(y_val_np.astype(int), ab_val.astype(int), p_est)
                    log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
                    nll = -float(np.mean(log_probs))
                    cv_scores.append(nll)
                avg_score = np.mean(cv_scores)
                if avg_score <= threshold:
                    best_k = k

        logger.info(f"    Optimal K={best_k} (Binom NLL: {best_score:.4f}, tolerance={self.tolerance:.3f})")
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
