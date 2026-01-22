import json
import logging
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_pinball_loss
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ImprovedFeatureSelector:
    """
    Robust feature selection pipeline for quantile regression.
    
    Improvements over v1:
    1. Optimizes for ALL quantiles (Average Pinball Loss), not just median.
    2. Uses Permutation Importance (on training data only) for ranking.
    3. Uses TimeSeriesSplit for validation to prevent leakage.
    4. Removes aggressive correlation pruning (XGBoost handles collinearity).
    """

    def __init__(self, n_splits: int = 3, quantiles: list = None):
        self.n_splits = n_splits
        self.quantiles = quantiles or [0.10, 0.25, 0.50, 0.75, 0.90]

    def select_features(
        self, 
        df: pd.DataFrame, 
        target_col: str, 
        candidate_cols: List[str],
        model_name: str = "Model"
    ) -> List[str]:
        """
        Run the feature selection pipeline.
        """
        logger.info(f"--- Feature Selection for {model_name} ---")
        logger.info(f"Target: {target_col}, Candidates: {len(candidate_cols)}")

        # 0. Preprocessing
        # Sort by date to ensure TimeSeriesSplit works correctly
        if "game_date" in df.columns:
            df = df.sort_values("game_date").reset_index(drop=True)
        
        valid_data = df.dropna(subset=[target_col] + candidate_cols).copy()
        
        if valid_data.empty:
            logger.warning(f"No valid data for {model_name}. Skipping.")
            return []

        X = valid_data[candidate_cols]
        y = valid_data[target_col]

        # 1. Rank Features (Permutation Importance)
        logger.info("Step 1: Ranking features (Permutation Importance)...")
        ranked_features = self._rank_features(X, y)
        logger.info(f"Top 5 features: {ranked_features[:5]}")

        # 2. Optimize Feature Count (Validation)
        logger.info("Step 2: Optimizing feature count (CV Average Pinball Loss)...")
        final_features = self._optimize_feature_count(X, y, ranked_features)
        
        logger.info(f"Selected {len(final_features)} features for {model_name}.")
        return final_features

    def _rank_features(self, X: pd.DataFrame, y: pd.Series) -> List[str]:
        """
        Rank features using Permutation Importance from an initial XGBoost model.
        Uses the last split of a TimeSeriesSplit to ensure we rank based on 
        generalizable patterns, or simply the first % of data to rank for the rest.
        
        To be robust, we'll use the train set of the LAST split (most data).
        """
        tscv = TimeSeriesSplit(n_splits=self.n_splits)
        # Get the indices for the last split
        train_index = []
        for train_index, _ in tscv.split(X):
            pass # We just want the last iteration
        
        if len(train_index) == 0:
            # Fallback if no splits (e.g. data too small)
            # Use first 80% as train
            split_idx = int(len(X) * 0.8)
            train_index = range(split_idx)
        
        X_train = X.iloc[train_index]
        y_train = y.iloc[train_index]

        # Train a proxy model (Median Regression) for ranking
        # Median is robust to outliers and good for quantile tasks
        model = xgb.XGBRegressor(
            objective="reg:quantileerror",
            quantile_alpha=0.50,
            n_estimators=100,
            learning_rate=0.05,
            max_depth=3,
            n_jobs=-1,
            random_state=42
        )
        model.fit(X_train, y_train)

        # Compute Permutation Importance
        result = permutation_importance(
            model, X_train, y_train, 
            n_repeats=5, 
            random_state=42, 
            n_jobs=-1,
            scoring="neg_mean_absolute_error" # Proxy for median loss
        )

        # Rank by mean importance (descending)
        importances = result.importances_mean
        feature_importance = pd.DataFrame({
            "feature": X.columns,
            "importance": importances
        })
        
        # Sort descending
        ranked = feature_importance.sort_values("importance", ascending=False)
        
        # Filter out features with <= 0 importance (noise)
        positive_features = ranked[ranked["importance"] > 0]["feature"].tolist()
        
        # If too few positive features, fall back to just the sorted list
        if len(positive_features) < 3:
            return ranked["feature"].tolist()
            
        return positive_features

    def _optimize_feature_count(
        self, 
        X: pd.DataFrame, 
        y: pd.Series, 
        ranked_candidates: List[str]
    ) -> List[str]:
        """
        Find optimal K by evaluating Average Pinball Loss across all quantiles
        using TimeSeriesSplit cross-validation.
        """
        tscv = TimeSeriesSplit(n_splits=self.n_splits)
        
        # Define search space: Top 5, 10, 15, ... up to Max
        step = 5
        max_feats = len(ranked_candidates)
        counts_to_test = list(range(5, max_feats + 1, step))
        if max_feats not in counts_to_test:
            counts_to_test.append(max_feats)
        
        # Ensure minimal set
        if max_feats < 5:
            counts_to_test = [max_feats]

        best_score = float("inf")
        best_k = counts_to_test[0]
        
        # Cache CV splits to avoid re-generating
        splits = list(tscv.split(X))

        for k in counts_to_test:
            current_features = ranked_candidates[:k]
            cv_scores = []
            
            for train_idx, val_idx in splits:
                X_train, X_val = X.iloc[train_idx][current_features], X.iloc[val_idx][current_features]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
                
                # Evaluate this feature set across ALL quantiles
                # We train a separate light model for each quantile to measure true aggregate loss
                fold_losses = []
                
                for q in self.quantiles:
                    model = xgb.XGBRegressor(
                        objective="reg:quantileerror",
                        quantile_alpha=q,
                        n_estimators=50, # Fast training for selection
                        max_depth=3,
                        learning_rate=0.1,
                        n_jobs=-1,
                        random_state=42
                    )
                    model.fit(X_train, y_train)
                    preds = model.predict(X_val)
                    
                    loss = mean_pinball_loss(y_val, preds, alpha=q)
                    fold_losses.append(loss)
                
                # Average loss across quantiles for this fold
                cv_scores.append(np.mean(fold_losses))
            
            # Average CV score for this K
            avg_score = np.mean(cv_scores)
            logger.info(f"  Top {k}: Avg Pinball Loss = {avg_score:.4f}")
            
            # Select best (Lower is better)
            # Add a slight penalty for complexity? No, pinball loss on validation is unbiased.
            if avg_score < best_score:
                best_score = avg_score
                best_k = k
        
        logger.info(f"Optimal feature count: {best_k} (Loss: {best_score:.4f})")
        return ranked_candidates[:best_k]


def get_candidate_columns(df: pd.DataFrame, target_col: str) -> List[str]:
    """
    Identify potential feature columns from the dataframe.
    """
    excluded_suffixes = ["_id", "_date", "_name", "season_id", "game_id", "team_id", "opponent_id", "player_id"]
    excluded_exact = [
        "actual_minutes", "actual_pts", "actual_reb", "actual_ast", "actual_threes", 
        "pts_per_min", "reb_per_min", "ast_per_min", "threes_per_min",
        "position_group"
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