# models/quantile_trainer.py

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.isotonic import IsotonicRegression
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import joblib
from pathlib import Path

@dataclass
class QuantileModelConfig:
    """Configuration for quantile model training."""
    quantiles: Tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)
    
    # XGBoost parameters
    n_estimators: int = 1000
    max_depth: int = 5
    learning_rate: float = 0.03
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 3
    early_stopping_rounds: int = 50
    
    # Training config
    val_fraction: float = 0.15
    random_state: int = 42


class QuantileModelSuite:
    """
    Trains and manages a suite of quantile regression models.
    """
    
    def __init__(self, config: Optional[QuantileModelConfig] = None):
        self.config = config or QuantileModelConfig()
        self.models: Dict[float, xgb.XGBRegressor] = {}
        self.feature_names: List[str] = []
        
    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        feature_names: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """
        Train separate models for each quantile.
        
        Returns dict of {quantile: validation_loss}.
        """
        self.feature_names = feature_names or list(X.columns)
        
        # Split for early stopping
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, 
            test_size=self.config.val_fraction,
            shuffle=False  # Preserve temporal order
        )
        
        results = {}
        
        for q in self.config.quantiles:
            print(f"\nTraining quantile {q:.2f}...")
            
            model = xgb.XGBRegressor(
                objective='reg:quantileerror',
                quantile_alpha=q,
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                min_child_weight=self.config.min_child_weight,
                random_state=self.config.random_state,
                n_jobs=-1,
            )
            
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False
            )
            
            # Evaluate calibration
            val_preds = model.predict(X_val)
            coverage = (y_val <= val_preds).mean()
            
            print(f"  Quantile {q:.2f}: Target coverage = {q:.2f}, "
                  f"Actual coverage = {coverage:.3f}")
            
            self.models[q] = model
            results[q] = coverage
        
        return results
    
    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Predict all quantiles for input data.
        
        Returns DataFrame with columns ['q10', 'q25', 'q50', 'q75', 'q90'].
        """
        predictions = {}
        
        for q, model in self.models.items():
            predictions[f'q{int(q*100):02d}'] = model.predict(X)
        
        result = pd.DataFrame(predictions)
        
        # Enforce monotonicity
        result = self._enforce_monotonicity(result)
        
        return result
    
    def _enforce_monotonicity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure quantile predictions are monotonically increasing.
        Uses isotonic regression row-by-row.
        """
        quantile_cols = sorted(df.columns)  # ['q10', 'q25', ...]
        quantile_values = [int(c[1:]) / 100 for c in quantile_cols]
        
        result = df.copy()
        
        for idx in df.index:
            values = df.loc[idx, quantile_cols].values
            
            # Check if already monotonic
            if np.all(np.diff(values) >= 0):
                continue
            
            # Apply isotonic regression
            ir = IsotonicRegression()
            fixed = ir.fit_transform(quantile_values, values)
            result.loc[idx, quantile_cols] = fixed
        
        return result
    
    def save(self, path: str):
        """Save all models to disk."""
        save_dict = {
            'models': {q: model for q, model in self.models.items()},
            'config': self.config,
            'feature_names': self.feature_names,
        }
        joblib.dump(save_dict, path)
        print(f"Saved quantile model suite to {path}")
    
    @classmethod
    def load(cls, path: str) -> 'QuantileModelSuite':
        """Load models from disk."""
        save_dict = joblib.load(path)
        
        suite = cls(config=save_dict['config'])
        suite.models = save_dict['models']
        suite.feature_names = save_dict['feature_names']
        
        return suite


class PlayerPropsModelPipeline:
    """
    Complete pipeline for training minutes and rate models.
    """
    
    def __init__(self, feature_store, config: Optional[QuantileModelConfig] = None):
        self.feature_store = feature_store
        self.config = config or QuantileModelConfig()
        
        # Model suites
        self.minutes_model: Optional[QuantileModelSuite] = None
        self.rate_models: Dict[str, QuantileModelSuite] = {}
        
        # Feature lists
        self.minutes_features: List[str] = []
        self.rate_features: List[str] = []
        
    def train_minutes_model(self, df: pd.DataFrame) -> Dict:
        """Train the minutes prediction model."""
        print("\n" + "="*60)
        print("TRAINING MINUTES MODEL")
        print("="*60)
        
        # Define minutes-specific features
        self.minutes_features = [
            # Player recent minutes
            'player_avg_min_l5', 'player_avg_min_l15', 'player_avg_min_szn',
            'player_games_l5', 'player_games_l15',
            
            # Player role indicators
            'player_avg_usg_pct_l5', 'player_avg_usg_pct_l15',
            
            # Team context
            'team_avg_pace_l5', 'team_avg_pace_l15',
            
            # Opponent context (affects game pace)
            'opp_avg_pace_l5', 'opp_avg_pace_l15',
            
            # Game context
            'is_home',
            'rest_days',
            'is_back_to_back',
            'line_spread',  # Blowout indicator
            'line_total',   # Pace indicator
        ]
        
        # Filter to available features
        available_features = [f for f in self.minutes_features if f in df.columns]
        print(f"Using {len(available_features)} features for minutes model")
        
        # Filter to valid rows (player played)
        valid_mask = df['actual_minutes'] > 0
        X = df.loc[valid_mask, available_features].fillna(0)
        y = df.loc[valid_mask, 'actual_minutes']
        
        print(f"Training on {len(X):,} samples")
        
        # Train
        self.minutes_model = QuantileModelSuite(self.config)
        results = self.minutes_model.train(X, y, available_features)
        
        return results
    
    def train_rate_models(self, df: pd.DataFrame, stats: List[str] = None) -> Dict:
        """Train rate models for each stat."""
        stats = stats or ['pts', 'reb', 'ast']
        
        print("\n" + "="*60)
        print("TRAINING RATE MODELS")
        print("="*60)
        
        # Define rate-specific features
        self.rate_features = [
            # Player efficiency
            'player_avg_usg_pct_l5', 'player_avg_usg_pct_l15', 'player_avg_usg_pct_szn',
            'player_avg_ts_pct_l5', 'player_avg_ts_pct_l15', 'player_avg_ts_pct_szn',
            'player_avg_off_rtg_l5', 'player_avg_off_rtg_l15',
            
            # Player averages (context for rate)
            'player_avg_pts_l5', 'player_avg_pts_l15',
            'player_avg_reb_l5', 'player_avg_reb_l15',
            'player_avg_ast_l5', 'player_avg_ast_l15',
            
            # Team context
            'team_avg_off_rtg_l5', 'team_avg_pace_l5',
            
            # Opponent overall defense
            'opp_avg_def_rtg_l5', 'opp_avg_def_rtg_l15',
            'opp_avg_pace_l5',
            
            # Opponent positional defense (KEY FEATURE)
            'opp_pos_pts_allowed_per36_l5', 'opp_pos_pts_allowed_per36_l15',
            'opp_pos_reb_allowed_per36_l5', 'opp_pos_reb_allowed_per36_l15',
            'opp_pos_ast_allowed_per36_l5', 'opp_pos_ast_allowed_per36_l15',
            
            # Game context
            'is_home',
            'line_total',  # Correlated with pace
        ]
        
        available_features = [f for f in self.rate_features if f in df.columns]
        print(f"Using {len(available_features)} features for rate models")
        
        all_results = {}
        
        for stat in stats:
            print(f"\n--- Training {stat.upper()} rate model ---")
            
            # Filter to valid rows (minimum minutes for rate calculation)
            rate_col = f'{stat}_per_min'
            valid_mask = df[rate_col].notna() & (df['actual_minutes'] >= 10)
            
            X = df.loc[valid_mask, available_features].fillna(0)
            y = df.loc[valid_mask, rate_col]
            
            print(f"Training on {len(X):,} samples")
            
            # Train
            model_suite = QuantileModelSuite(self.config)
            results = model_suite.train(X, y, available_features)
            
            self.rate_models[stat] = model_suite
            all_results[stat] = results
        
        return all_results
    
    def save_all(self, directory: str):
        """Save all models."""
        path = Path(directory)
        path.mkdir(exist_ok=True)
        
        if self.minutes_model:
            self.minutes_model.save(path / 'minutes_model.joblib')
        
        for stat, model in self.rate_models.items():
            model.save(path / f'{stat}_rate_model.joblib')
        
        # Save feature lists
        joblib.dump({
            'minutes_features': self.minutes_features,
            'rate_features': self.rate_features,
        }, path / 'feature_config.joblib')
        
        print(f"\nAll models saved to {directory}")
    
    @classmethod
    def load_all(cls, directory: str, feature_store) -> 'PlayerPropsModelPipeline':
        """Load all models."""
        path = Path(directory)
        
        pipeline = cls(feature_store)
        
        # Load minutes model
        minutes_path = path / 'minutes_model.joblib'
        if minutes_path.exists():
            pipeline.minutes_model = QuantileModelSuite.load(minutes_path)
        
        # Load rate models
        for stat in ['pts', 'reb', 'ast', 'stl', 'blk']:
            rate_path = path / f'{stat}_rate_model.joblib'
            if rate_path.exists():
                pipeline.rate_models[stat] = QuantileModelSuite.load(rate_path)
        
        # Load feature config
        config = joblib.load(path / 'feature_config.joblib')
        pipeline.minutes_features = config['minutes_features']
        pipeline.rate_features = config['rate_features']
        
        return pipeline