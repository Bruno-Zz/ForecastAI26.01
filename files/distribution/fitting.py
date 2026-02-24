"""
Distribution Fitting Module for MEIO
Fits parametric distributions to forecast quantiles for inventory optimization
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
import yaml
from pathlib import Path
from dataclasses import dataclass, asdict
from scipy import stats
from scipy.optimize import minimize


@dataclass
class FittedDistribution:
    """Container for fitted distribution parameters."""
    unique_id: str
    method: str
    forecast_horizon: int
    
    # Distribution type
    distribution_type: str  # 'normal', 'gamma', 'negative_binomial', 'lognormal'
    
    # Parameters
    mean: float
    std: float
    params: Dict  # Distribution-specific parameters
    
    # Goodness of fit
    ks_statistic: Optional[float] = None
    ks_pvalue: Optional[float] = None
    
    # For MEIO calculations
    service_level_quantiles: Optional[Dict[float, float]] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary, ensuring dict keys are strings for Parquet compatibility."""
        d = asdict(self)
        # PyArrow requires string keys in dict columns — convert float quantile keys
        if d.get('service_level_quantiles'):
            d['service_level_quantiles'] = {
                str(k): v for k, v in d['service_level_quantiles'].items()
            }
        return d
    
    def get_quantile(self, q: float) -> float:
        """
        Get quantile value from fitted distribution.
        
        Args:
            q: Quantile level (0 to 1)
            
        Returns:
            Quantile value
        """
        if self.distribution_type == 'normal':
            return stats.norm.ppf(q, loc=self.mean, scale=self.std)
        
        elif self.distribution_type == 'gamma':
            shape = self.params['shape']
            scale = self.params['scale']
            return stats.gamma.ppf(q, a=shape, scale=scale)
        
        elif self.distribution_type == 'negative_binomial':
            n = self.params['n']
            p = self.params['p']
            return stats.nbinom.ppf(q, n=n, p=p)
        
        elif self.distribution_type == 'lognormal':
            s = self.params['s']
            scale = self.params['scale']
            return stats.lognorm.ppf(q, s=s, scale=scale)
        
        else:
            raise ValueError(f"Unknown distribution: {self.distribution_type}")


class DistributionFitter:
    """
    Fits parametric distributions to forecast quantiles for MEIO.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.meio_config = self.config.get('meio', {})
        self.logger = logging.getLogger(__name__)
        
        # Extract configuration
        self.enabled = self.meio_config.get('enabled', True)
        self.distributions = self.meio_config.get('distributions', 
                                                   ['normal', 'gamma', 'negative_binomial', 'lognormal'])
        self.fitting_method = self.meio_config.get('fitting_method', 'quantile_matching')
        self.service_levels = self.meio_config.get('service_levels', [0.90, 0.95, 0.99])
    
    def fit_from_quantiles(self,
                          quantiles: Dict[float, float],
                          distribution_type: str = 'auto') -> Optional[FittedDistribution]:
        """
        Fit parametric distribution from forecast quantiles.
        
        Args:
            quantiles: Dictionary mapping quantile levels to values
            distribution_type: Type of distribution or 'auto' for automatic selection
            
        Returns:
            FittedDistribution object or None if fitting fails
        """
        if distribution_type == 'auto':
            # Try all distributions and select best fit
            best_fit = None
            best_score = float('inf')
            
            for dist_type in self.distributions:
                try:
                    fit = self._fit_distribution(quantiles, dist_type)
                    if fit is not None:
                        score = self._score_fit(quantiles, fit)
                        if score < best_score:
                            best_score = score
                            best_fit = fit
                except Exception as e:
                    self.logger.debug(f"Failed to fit {dist_type}: {e}")
                    continue
            
            return best_fit
        else:
            return self._fit_distribution(quantiles, distribution_type)
    
    def _fit_distribution(self,
                         quantiles: Dict[float, float],
                         distribution_type: str) -> Optional[FittedDistribution]:
        """
        Fit a specific distribution type to quantiles.
        """
        if self.fitting_method == 'quantile_matching':
            return self._fit_by_quantile_matching(quantiles, distribution_type)
        elif self.fitting_method == 'mle':
            # Would need actual samples for MLE
            self.logger.warning("MLE fitting requires samples, using quantile matching")
            return self._fit_by_quantile_matching(quantiles, distribution_type)
        else:
            raise ValueError(f"Unknown fitting method: {self.fitting_method}")
    
    def _fit_by_quantile_matching(self,
                                  quantiles: Dict[float, float],
                                  distribution_type: str) -> Optional[FittedDistribution]:
        """
        Fit distribution by matching theoretical quantiles to empirical quantiles.
        """
        # Extract quantile levels and values
        q_levels = np.array(sorted(quantiles.keys()))
        q_values = np.array([quantiles[q] for q in q_levels])
        
        # Filter out invalid values
        valid_idx = np.isfinite(q_values) & (q_values >= 0)
        q_levels = q_levels[valid_idx]
        q_values = q_values[valid_idx]
        
        if len(q_levels) < 2:
            return None
        
        try:
            if distribution_type == 'normal':
                return self._fit_normal(q_levels, q_values)
            
            elif distribution_type == 'gamma':
                return self._fit_gamma(q_levels, q_values)
            
            elif distribution_type == 'negative_binomial':
                return self._fit_negative_binomial(q_levels, q_values)
            
            elif distribution_type == 'lognormal':
                return self._fit_lognormal(q_levels, q_values)
            
            else:
                raise ValueError(f"Unknown distribution type: {distribution_type}")
        
        except Exception as e:
            self.logger.warning(f"Failed to fit {distribution_type}: {e}")
            return None
    
    def _fit_normal(self, q_levels: np.ndarray, q_values: np.ndarray) -> FittedDistribution:
        """Fit normal distribution."""
        # Use median and IQR for robust parameter estimation
        if 0.5 in q_levels:
            median_idx = np.where(q_levels == 0.5)[0][0]
            mean = q_values[median_idx]
        else:
            mean = np.median(q_values)
        
        # Estimate std from IQR
        if 0.25 in q_levels and 0.75 in q_levels:
            q25_idx = np.where(q_levels == 0.25)[0][0]
            q75_idx = np.where(q_levels == 0.75)[0][0]
            iqr = q_values[q75_idx] - q_values[q25_idx]
            std = iqr / 1.349  # IQR = 1.349 * std for normal distribution
        else:
            # Use quantile spread
            std = (q_values[-1] - q_values[0]) / (stats.norm.ppf(q_levels[-1]) - stats.norm.ppf(q_levels[0]))
        
        std = max(std, 0.001)  # Avoid zero std
        
        return FittedDistribution(
            unique_id="",
            method="",
            forecast_horizon=0,
            distribution_type='normal',
            mean=float(mean),
            std=float(std),
            params={'loc': float(mean), 'scale': float(std)}
        )
    
    def _fit_gamma(self, q_levels: np.ndarray, q_values: np.ndarray) -> FittedDistribution:
        """Fit gamma distribution."""
        # Gamma distribution: shape (α), scale (θ)
        # Mean = α * θ, Variance = α * θ²
        
        def objective(params):
            shape, scale = params
            if shape <= 0 or scale <= 0:
                return 1e10
            
            theoretical_quantiles = stats.gamma.ppf(q_levels, a=shape, scale=scale)
            return np.sum((theoretical_quantiles - q_values) ** 2)
        
        # Initial guess
        mean_est = np.mean(q_values)
        var_est = np.var(q_values)
        shape_init = (mean_est ** 2) / max(var_est, 0.001)
        scale_init = max(var_est, 0.001) / mean_est
        
        result = minimize(objective, x0=[shape_init, scale_init], 
                         bounds=[(0.01, None), (0.01, None)],
                         method='L-BFGS-B')
        
        shape, scale = result.x
        mean = shape * scale
        std = np.sqrt(shape) * scale
        
        return FittedDistribution(
            unique_id="",
            method="",
            forecast_horizon=0,
            distribution_type='gamma',
            mean=float(mean),
            std=float(std),
            params={'shape': float(shape), 'scale': float(scale)}
        )
    
    def _fit_negative_binomial(self, q_levels: np.ndarray, q_values: np.ndarray) -> FittedDistribution:
        """Fit negative binomial distribution (for count data)."""
        # Negative binomial: n (number of successes), p (probability)
        # Mean = n * (1-p) / p, Variance = n * (1-p) / p²
        
        def objective(params):
            n, p = params
            if n <= 0 or p <= 0 or p >= 1:
                return 1e10
            
            theoretical_quantiles = stats.nbinom.ppf(q_levels, n=n, p=p)
            return np.sum((theoretical_quantiles - q_values) ** 2)
        
        # Initial guess
        mean_est = np.mean(q_values)
        var_est = np.var(q_values)
        
        if var_est > mean_est:  # Overdispersion
            p_init = mean_est / var_est
            n_init = mean_est * p_init / (1 - p_init)
        else:
            p_init = 0.5
            n_init = mean_est
        
        result = minimize(objective, x0=[n_init, p_init],
                         bounds=[(0.01, None), (0.01, 0.99)],
                         method='L-BFGS-B')
        
        n, p = result.x
        mean = n * (1 - p) / p
        std = np.sqrt(n * (1 - p)) / p
        
        return FittedDistribution(
            unique_id="",
            method="",
            forecast_horizon=0,
            distribution_type='negative_binomial',
            mean=float(mean),
            std=float(std),
            params={'n': float(n), 'p': float(p)}
        )
    
    def _fit_lognormal(self, q_levels: np.ndarray, q_values: np.ndarray) -> FittedDistribution:
        """Fit lognormal distribution."""
        # Lognormal: s (shape), scale
        
        # Transform to log space
        log_values = np.log(q_values + 1e-10)  # Avoid log(0)
        
        def objective(params):
            s, scale = params
            if s <= 0 or scale <= 0:
                return 1e10
            
            theoretical_quantiles = stats.lognorm.ppf(q_levels, s=s, scale=scale)
            return np.sum((theoretical_quantiles - q_values) ** 2)
        
        # Initial guess from log-space moments
        mu = np.mean(log_values)
        sigma = np.std(log_values)
        
        result = minimize(objective, x0=[sigma, np.exp(mu)],
                         bounds=[(0.01, None), (0.01, None)],
                         method='L-BFGS-B')
        
        s, scale = result.x
        mean = scale * np.exp(s ** 2 / 2)
        std = scale * np.exp(s ** 2 / 2) * np.sqrt(np.exp(s ** 2) - 1)
        
        return FittedDistribution(
            unique_id="",
            method="",
            forecast_horizon=0,
            distribution_type='lognormal',
            mean=float(mean),
            std=float(std),
            params={'s': float(s), 'scale': float(scale)}
        )
    
    def _score_fit(self, quantiles: Dict[float, float], fit: FittedDistribution) -> float:
        """
        Score the quality of a distribution fit.
        Lower is better.
        """
        errors = []
        for q_level, q_value in quantiles.items():
            predicted = fit.get_quantile(q_level)
            error = abs(predicted - q_value)
            errors.append(error)
        
        return np.mean(errors)
    
    def calculate_service_level_quantiles(self, fit: FittedDistribution) -> Dict[float, float]:
        """
        Calculate quantiles for standard service levels (for MEIO).
        
        Args:
            fit: Fitted distribution
            
        Returns:
            Dictionary mapping service levels to demand quantiles
        """
        quantiles = {}
        
        for service_level in self.service_levels:
            quantiles[service_level] = fit.get_quantile(service_level)
        
        return quantiles
    
    def fit_forecast_distributions(self,
                                  forecasts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fit distributions to all forecasts in a DataFrame.
        
        Args:
            forecasts_df: DataFrame with forecast results (including quantiles)
            
        Returns:
            DataFrame with fitted distributions
        """
        distributions = []
        
        for _, row in forecasts_df.iterrows():
            unique_id = row['unique_id']
            method = row['method']
            
            # Extract quantiles
            quantiles_dict = row['quantiles']
            if isinstance(quantiles_dict, str):
                import json
                quantiles_dict = json.loads(quantiles_dict.replace("'", '"'))
            
            # Convert to numeric — quantile values may be full-horizon arrays
            # (one value per forecast step).  Reduce to a scalar by taking the
            # mean across the horizon so the fitted distribution represents the
            # average-period demand (the API scales it per-horizon from there).
            quantiles = {}
            for k, v in quantiles_dict.items():
                try:
                    arr = np.asarray(v, dtype=float)
                    quantiles[float(k)] = float(arr.mean()) if arr.ndim > 0 and arr.size > 1 else float(arr)
                except (TypeError, ValueError):
                    continue
            
            if len(quantiles) < 2:
                continue

            # Fit distribution
            fit = self.fit_from_quantiles(quantiles, distribution_type='auto')
            
            if fit is not None:
                # Update metadata
                fit.unique_id = unique_id
                fit.method = method
                fit.forecast_horizon = len(row['point_forecast'])
                
                # Calculate service level quantiles
                fit.service_level_quantiles = self.calculate_service_level_quantiles(fit)
                
                distributions.append(fit.to_dict())
        
        return pd.DataFrame(distributions)


def main():
    """Example usage of distribution fitter."""
    from db.db import load_table, get_schema, bulk_insert, jsonb_serialize

    config_path = 'config/config.yaml'
    schema = get_schema(config_path)

    # Load forecasts from PostgreSQL
    forecasts_df = load_table(config_path, f"{schema}.forecast_results")
    if forecasts_df.empty:
        print("No forecasts found in DB. Run forecasting first.")
        return

    # Initialize fitter
    fitter = DistributionFitter()

    # Fit distributions
    distributions_df = fitter.fit_forecast_distributions(forecasts_df)

    # Save results to PostgreSQL
    if not distributions_df.empty:
        cols = list(distributions_df.columns)
        rows = [
            tuple(jsonb_serialize(v) for v in row)
            for row in distributions_df.itertuples(index=False, name=None)
        ]
        n = bulk_insert(config_path, f"{schema}.fitted_distributions", cols, rows)
        print(f"\nFitted distributions saved to {schema}.fitted_distributions ({n} rows)")
    print(f"Generated {len(distributions_df)} distribution fits")

    # Summary
    print("\nDistribution type summary:")
    print(distributions_df['distribution_type'].value_counts())

    print("\nExample service level quantiles:")
    print(distributions_df[['unique_id', 'method', 'distribution_type', 'service_level_quantiles']].head())


if __name__ == "__main__":
    main()
