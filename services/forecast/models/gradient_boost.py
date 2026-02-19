"""
MetalLedger — Gradient Boosting forecast model.

Uses scikit-learn GradientBoostingRegressor with lag features.
If LightGBM is installed, prefers it for speed.

Feature engineering:
  - Lag features: price[t-1], price[t-2], ..., price[t-LAG_WINDOW]
  - Rolling mean over 5 and 10 days
  - Rolling std over 5 days
  - Day-of-week (0–6)

P10/P50/P90 are estimated via quantile regression or bootstrap residuals.
"""

from __future__ import annotations

import math
from typing import List, Optional, Tuple

import numpy as np

from common.logging_util import get_logger

log = get_logger(__name__)

LAG_WINDOW    = 7
MIN_OBS       = LAG_WINDOW + 5   # minimum observations to fit


def _build_features(prices: List[float]) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build (X, y) feature matrix and target vector from price series.

    Each sample corresponds to predicting prices[t] from prices[t-LAG_WINDOW:t].
    """
    X_rows, y_vals = [], []
    arr = np.array(prices)

    for i in range(LAG_WINDOW, len(arr)):
        lags     = arr[i - LAG_WINDOW:i].tolist()
        roll5    = float(np.mean(arr[max(0, i-5):i]))
        roll10   = float(np.mean(arr[max(0, i-10):i]))
        std5     = float(np.std(arr[max(0, i-5):i])) if i >= 5 else 0.0
        dow      = float(i % 7)                          # proxy for day-of-week

        feats = lags + [roll5, roll10, std5, dow]
        X_rows.append(feats)
        y_vals.append(arr[i])

    return np.array(X_rows), np.array(y_vals)


def _predict_horizon(
    model,
    last_prices: List[float],
    horizon: int,
) -> List[float]:
    """
    Iteratively predict `horizon` steps by appending each prediction.
    """
    history = list(last_prices)
    preds   = []
    arr     = np.array(history)

    for _ in range(horizon):
        lags  = arr[-LAG_WINDOW:].tolist()
        roll5 = float(np.mean(arr[-5:])) if len(arr) >= 5 else float(np.mean(arr))
        roll10= float(np.mean(arr[-10:])) if len(arr) >= 10 else float(np.mean(arr))
        std5  = float(np.std(arr[-5:])) if len(arr) >= 5 else 0.0
        dow   = float(len(preds) % 7)
        feat  = np.array([lags + [roll5, roll10, std5, dow]])

        pred = float(model.predict(feat)[0])
        preds.append(pred)
        arr = np.append(arr, pred)

    return preds


def _fit_and_predict_sklearn(
    prices: List[float],
    horizon: int,
) -> dict:
    """Fit GradientBoostingRegressor and predict for horizon."""
    from sklearn.ensemble import GradientBoostingRegressor

    X, y = _build_features(prices)
    if len(X) < 5:
        return {"p10": None, "p50": None, "p90": None}

    # Train three quantile regressors for P10, P50, P90
    results_by_quantile = {}
    for alpha, label in [(0.1, "p10"), (0.5, "p50"), (0.9, "p90")]:
        gbr = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            loss="quantile",
            alpha=alpha,
            random_state=42,
        )
        gbr.fit(X, y)
        preds = _predict_horizon(gbr, prices, horizon)
        results_by_quantile[label] = max(round(preds[-1], 6), 0.0)

    return results_by_quantile


def _fit_and_predict_lightgbm(prices: List[float], horizon: int) -> dict:
    """Fit LightGBM with quantile objectives."""
    import lightgbm as lgb

    X, y = _build_features(prices)
    if len(X) < 5:
        return {"p10": None, "p50": None, "p90": None}

    results = {}
    for alpha, label in [(0.1, "p10"), (0.5, "p50"), (0.9, "p90")]:
        params = {
            "objective":  "quantile",
            "alpha":      alpha,
            "num_leaves": 15,
            "n_estimators": 100,
            "learning_rate": 0.1,
            "verbose": -1,
        }
        model = lgb.LGBMRegressor(**params)
        model.fit(X, y)
        preds = _predict_horizon(model, prices, horizon)
        results[label] = max(round(preds[-1], 6), 0.0)

    return results


def run(
    prices: List[float],
    horizons: List[int] = [1, 5, 20],
) -> dict:
    """
    Run gradient boosting quantile forecast.

    Args:
        prices:   Historical prices, ascending by date.
        horizons: Forecast horizons in days.

    Returns:
        Dict mapping horizon → {"p10": float, "p50": float, "p90": float}
    """
    if len(prices) < MIN_OBS:
        log.warning(
            "GradientBoost requires >= %d obs, got %d — returning None",
            MIN_OBS, len(prices),
        )
        return {h: {"p10": None, "p50": None, "p90": None} for h in horizons}

    results = {}
    for h in horizons:
        try:
            try:
                import lightgbm  # noqa: F401
                res = _fit_and_predict_lightgbm(prices, h)
            except ImportError:
                res = _fit_and_predict_sklearn(prices, h)

            # Fallback if any quantile is None
            if any(v is None for v in res.values()):
                raise ValueError("Incomplete quantile predictions")

            results[h] = res
            log.debug(
                "GBM h=%d → P10=%.4f P50=%.4f P90=%.4f",
                h, res["p10"], res["p50"], res["p90"],
            )
        except Exception as exc:
            log.error("GradientBoost h=%d failed: %s — returning None", h, exc)
            results[h] = {"p10": None, "p50": None, "p90": None}

    return results
