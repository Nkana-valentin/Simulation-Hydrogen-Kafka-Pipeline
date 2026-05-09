import datetime as dt

import pytest

from domain.quality import (
    clean_record,
    content_completeness,
    evaluate_dimensions,
    hampel_outlier_ratio,
    longitudinal_wqs,
    timeliness_score,
    validate_record,
    weighted_quality_score,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record(**kwargs):
    base = {"timestamp": "2026-01-01T00:00:00.000Z", "H2_001PT": 1.5}
    base.update(kwargs)
    return base


def _now_ts() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ---------------------------------------------------------------------------
# validate_record
# ---------------------------------------------------------------------------

def test_valid_record_passes():
    result = validate_record(_record())
    assert result.is_valid
    assert result.errors == []


def test_missing_timestamp_fails():
    result = validate_record({"H2_001PT": 1.5})
    assert not result.is_valid
    assert any("timestamp" in e for e in result.errors)


def test_future_timestamp_fails():
    result = validate_record(_record(timestamp="2099-01-01T00:00:00.000Z"))
    assert not result.is_valid
    assert any("future" in e for e in result.errors)


def test_nan_value_fails():
    result = validate_record(_record(H2_001PT=float("nan")))
    assert not result.is_valid
    assert any("NaN" in e for e in result.errors)


def test_inf_value_fails():
    result = validate_record(_record(H2_001PT=float("inf")))
    assert not result.is_valid
    assert any("infinite" in e for e in result.errors)


# ---------------------------------------------------------------------------
# content_completeness
# ---------------------------------------------------------------------------

def test_content_completeness_full():
    assert content_completeness({"a": 1, "b": 2}) == 1.0


def test_content_completeness_partial():
    score = content_completeness({"a": 1, "b": None})
    assert score == 0.5


def test_content_completeness_all_null():
    assert content_completeness({"a": None, "b": None}) == 0.0


def test_content_completeness_empty():
    assert content_completeness({}) == 0.0


# ---------------------------------------------------------------------------
# timeliness_score
# ---------------------------------------------------------------------------

def test_timeliness_recent():
    score = timeliness_score(_now_ts())
    assert score > 0.9


def test_timeliness_old():
    score = timeliness_score("2020-01-01T00:00:00.000Z")
    assert score == 0.0


def test_timeliness_exactly_at_window():
    # 120 s ago → score should be 0
    ts = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=120)).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )
    assert timeliness_score(ts) == 0.0


def test_timeliness_invalid_timestamp():
    assert timeliness_score("not-a-date") == 0.0


# ---------------------------------------------------------------------------
# hampel_outlier_ratio
# ---------------------------------------------------------------------------

def test_hampel_no_outliers():
    values = [1.0] * 20
    assert hampel_outlier_ratio(values) == 0.0


def test_hampel_obvious_outlier():
    values = [1.0] * 19 + [1000.0]
    ratio = hampel_outlier_ratio(values)
    assert ratio > 0.0


def test_hampel_all_outliers_constant_window():
    # All same value → MAD = 0 → no outliers detected (degenerate case)
    values = [5.0] * 10
    assert hampel_outlier_ratio(values) == 0.0


def test_hampel_too_few_values():
    # Needs at least 5 values; fewer returns 0.0 by design
    assert hampel_outlier_ratio([1.0, 2.0]) == 0.0


def test_hampel_returns_fraction():
    values = [1.0] * 18 + [1000.0, 2000.0]
    ratio = hampel_outlier_ratio(values)
    assert 0.0 < ratio <= 1.0


# ---------------------------------------------------------------------------
# weighted_quality_score
# ---------------------------------------------------------------------------

def test_wqs_perfect():
    assert weighted_quality_score(1.0, 1.0) == pytest.approx(1.0)


def test_wqs_zero():
    assert weighted_quality_score(0.0, 0.0) == pytest.approx(0.0)


def test_wqs_weights():
    # w_a=0.7, w_c=0.3 by default
    assert weighted_quality_score(1.0, 0.0) == pytest.approx(0.7)
    assert weighted_quality_score(0.0, 1.0) == pytest.approx(0.3)


def test_wqs_custom_weights():
    assert weighted_quality_score(1.0, 0.0, wa=0.5, wc=0.5) == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# longitudinal_wqs
# ---------------------------------------------------------------------------

def test_lwqs_empty_history():
    assert longitudinal_wqs([]) == 0.0


def test_lwqs_single_value():
    assert longitudinal_wqs([0.9]) == pytest.approx(0.9)


def test_lwqs_constant_series():
    # Weighted mean of a constant series equals that constant
    assert longitudinal_wqs([0.8] * 10) == pytest.approx(0.8, abs=1e-6)


def test_lwqs_recent_bias():
    # More recent (last) values carry higher weight with β=5
    history_rising = [0.5, 0.6, 0.7, 0.8, 0.9]
    history_falling = [0.9, 0.8, 0.7, 0.6, 0.5]
    assert longitudinal_wqs(history_rising) > longitudinal_wqs(history_falling)


# ---------------------------------------------------------------------------
# clean_record
# ---------------------------------------------------------------------------

def test_clean_record_replaces_nan():
    record = {"timestamp": _now_ts(), "H2_001PT": float("nan")}
    history = {"H2_001PT": 4.5}
    cleaned, imputed = clean_record(record, history)
    assert cleaned["H2_001PT"] == pytest.approx(4.5)
    assert "H2_001PT" in imputed


def test_clean_record_replaces_inf():
    record = {"timestamp": _now_ts(), "H2_001PT": float("inf")}
    history = {"H2_001PT": 3.0}
    cleaned, imputed = clean_record(record, history)
    assert cleaned["H2_001PT"] == pytest.approx(3.0)
    assert "H2_001PT" in imputed


def test_clean_record_no_history_leaves_nan():
    record = {"timestamp": _now_ts(), "H2_001PT": float("nan")}
    cleaned, imputed = clean_record(record, {})
    assert imputed == []


def test_clean_record_good_values_untouched():
    record = {"timestamp": _now_ts(), "H2_001PT": 4.5, "H2_002PT": 3.0}
    cleaned, imputed = clean_record(record, {"H2_001PT": 99.0})
    assert cleaned["H2_001PT"] == pytest.approx(4.5)
    assert imputed == []


# ---------------------------------------------------------------------------
# evaluate_dimensions — integration
# ---------------------------------------------------------------------------

def test_evaluate_dimensions_returns_all_keys():
    field_history: dict = {}
    wqs_history: list = []
    record = {**_record(timestamp=_now_ts()), "H2_002PT": 2.0}
    dims = evaluate_dimensions(record, field_history, wqs_history, processed_count=1)
    expected = {"accuracy", "completeness", "temporal_completeness", "timeliness", "wqs", "lwqs", "qsd"}
    assert expected == set(dims.keys())


def test_evaluate_dimensions_scores_in_range():
    field_history: dict = {}
    wqs_history: list = []
    record = {**_record(timestamp=_now_ts()), "H2_002PT": 2.0}
    dims = evaluate_dimensions(record, field_history, wqs_history, processed_count=1)
    for key, val in dims.items():
        assert -1.0 <= val <= 1.0, f"{key}={val} out of expected range"


def test_evaluate_dimensions_temporal_completeness_ramps():
    field_history: dict = {}
    wqs_history: list = []
    dims_early = evaluate_dimensions(
        _record(timestamp=_now_ts()), field_history, wqs_history, processed_count=10
    )
    dims_full = evaluate_dimensions(
        _record(timestamp=_now_ts()), field_history, wqs_history, processed_count=60
    )
    assert dims_early["temporal_completeness"] < dims_full["temporal_completeness"]


def test_evaluate_dimensions_qsd_definition():
    field_history: dict = {}
    wqs_history: list = []
    dims = evaluate_dimensions(
        _record(timestamp=_now_ts()), field_history, wqs_history, processed_count=1
    )
    assert dims["qsd"] == pytest.approx(dims["wqs"] - dims["lwqs"], abs=1e-9)


def test_evaluate_dimensions_mutates_histories():
    field_history: dict = {}
    wqs_history: list = []
    evaluate_dimensions(_record(timestamp=_now_ts()), field_history, wqs_history, processed_count=1)
    assert len(wqs_history) == 1
    assert "H2_001PT" in field_history


def test_evaluate_dimensions_window_capped_at_60():
    field_history: dict = {}
    wqs_history: list = []
    for i in range(70):
        evaluate_dimensions(
            _record(timestamp=_now_ts()), field_history, wqs_history, processed_count=i + 1
        )
    assert len(wqs_history) == 60
    for hist in field_history.values():
        assert len(hist) <= 60
