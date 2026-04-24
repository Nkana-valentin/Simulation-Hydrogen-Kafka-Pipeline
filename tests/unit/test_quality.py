from domain.quality import content_completeness, timeliness_score, validate_record


def _record(**kwargs):
    base = {"timestamp": "2026-01-01T00:00:00.000Z", "H2_001PT": 1.5}
    base.update(kwargs)
    return base


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


def test_content_completeness_full():
    assert content_completeness({"a": 1, "b": 2}) == 1.0


def test_content_completeness_partial():
    score = content_completeness({"a": 1, "b": None})
    assert score == 0.5


def test_timeliness_recent():
    import datetime as dt
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    score = timeliness_score(ts)
    assert score > 0.9


def test_timeliness_old():
    score = timeliness_score("2020-01-01T00:00:00.000Z")
    assert score == 0.0
