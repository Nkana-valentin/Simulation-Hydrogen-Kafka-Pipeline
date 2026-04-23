"""
Pure domain quality rules.  No I/O, no framework imports.
Implements the quality dimensions from Peixoto et al. (2025).
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import math
import numpy as np


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Low-level checks
# ---------------------------------------------------------------------------

def _parse_timestamp(ts: str) -> datetime:
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def validate_record(record: Dict[str, Any]) -> ValidationResult:
    """
    Validate a single telemetry record.
    Returns a ValidationResult with all detected problems.
    """
    errors: List[str] = []

    # Timestamp presence and validity
    ts_raw = record.get("timestamp")
    if not ts_raw:
        errors.append("Missing required field: timestamp")
    else:
        try:
            dt = _parse_timestamp(str(ts_raw))
            if dt > datetime.now(timezone.utc):
                errors.append(f"Timestamp is in the future: {ts_raw}")
        except (ValueError, TypeError):
            errors.append(f"Invalid timestamp format: {ts_raw}")

    # NaN / Inf checks on all numeric fields
    for key, value in record.items():
        if isinstance(value, float):
            if math.isnan(value):
                errors.append(f"{key} contains NaN")
            elif not math.isfinite(value):
                errors.append(f"{key} contains infinite value")

    return ValidationResult(is_valid=len(errors) == 0, errors=errors)


# ---------------------------------------------------------------------------
# Quality metrics (stateful helper — kept pure, caller owns state)
# ---------------------------------------------------------------------------

def hampel_outlier_ratio(values: List[float], threshold: float = 3.0) -> float:
    """
    Return fraction of outliers detected by Hampel filter (0 = no outliers).
    """
    if len(values) < 5:
        return 0.0
    arr = np.array(values)
    median = np.median(arr)
    mad = np.median(np.abs(arr - median))
    lower, upper = median - threshold * mad, median + threshold * mad
    n_outliers = int(np.sum((arr < lower) | (arr > upper)))
    return n_outliers / len(values)


def content_completeness(record: Dict[str, Any]) -> float:
    total = len(record)
    if total == 0:
        return 0.0
    missing = sum(1 for v in record.values() if v is None)
    return (total - missing) / total


def timeliness_score(ts: str, volatility_seconds: float = 120.0) -> float:
    try:
        dt = _parse_timestamp(ts)
        age = (datetime.now(timezone.utc) - dt).total_seconds()
        return max(0.0, 1.0 - age / volatility_seconds)
    except (ValueError, TypeError):
        return 0.0


def clean_record(
    record: Dict[str, Any],
    last_good_values: Dict[str, float],
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Replace NaN/Inf float fields with the last known good value for that field.
    Fields with no history are left unchanged (validate_record will reject them).
    Returns (cleaned_record, list_of_imputed_field_names).
    """
    cleaned = dict(record)
    imputed: List[str] = []
    for key, value in record.items():
        if key == "timestamp" or not isinstance(value, float):
            continue
        if math.isnan(value) or not math.isfinite(value):
            if key in last_good_values:
                cleaned[key] = last_good_values[key]
                imputed.append(key)
    return cleaned, imputed


def weighted_quality_score(
    accuracy: float,
    completeness: float,
    wa: float = 0.7,
    wc: float = 0.3,) -> float:
    return wa * accuracy + wc * completeness


def longitudinal_wqs(history: List[float], beta: int = 5) -> float:
    if not history:
        return 0.0
    n = len(history)
    weights = np.array([np.exp(-(n - k - 1) / beta) for k in range(n)])
    weights /= weights.sum()
    return float(np.dot(np.array(history), weights))


def evaluate_dimensions(
    record: Dict[str, Any],
    field_history: Dict[str, List[float]],
    wqs_history: List[float],
    processed_count: int,
    window_size: int = 60,
) -> Dict[str, float]:
    """
    Compute all quality dimensions for one record.
    Caller is responsible for maintaining the mutable histories.
    """
    numeric_values: List[float] = []
    for key, val in record.items():
        if key == "timestamp" or not isinstance(val, (int, float)):
            continue
        try:
            fval = float(val)
        except (TypeError, ValueError):
            continue
        numeric_values.append(fval)
        hist = field_history.setdefault(key, [])
        hist.append(fval)
        if len(hist) > window_size:
            hist.pop(0)

    accuracy_scores = [
        1.0 - hampel_outlier_ratio(hist)
        for hist in field_history.values()
        if hist
    ]
    accuracy = float(np.mean(accuracy_scores)) if accuracy_scores else 0.0
    completeness = content_completeness(record)
    timeliness = timeliness_score(record.get("timestamp", ""))

    wqs = weighted_quality_score(accuracy, completeness)
    lwqs = longitudinal_wqs(wqs_history)
    qsd = wqs - lwqs

    wqs_history.append(wqs)
    if len(wqs_history) > window_size:
        wqs_history.pop(0)

    return {
        "accuracy": accuracy,
        "completeness": completeness,
        "temporal_completeness": min(1.0, processed_count / window_size),
        "timeliness": timeliness,
        "wqs": wqs,
        "lwqs": lwqs,
        "qsd": qsd,
    }
