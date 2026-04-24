
from domain.auth import create_token, verify_researcher_token, verify_token

SECRET = "test-secret-do-not-use-in-prod-ok"  # 34 bytes — above HS256 minimum
ALGO = "HS256"


def _device_payload():
    return {"identity_type": "device", "device_id": "sensor_01", "lab": "APSU"}


def _researcher_payload():
    return {
        "identity_type": "researcher",
        "user_id": "APSU_dr_smith",
        "username": "dr_smith",
        "institution": "APSU",
        "roles": ["scientist"],
        "data_access": ["pressure"],
    }


def test_create_and_verify_device_token():
    token = create_token(_device_payload(), SECRET, ALGO)
    result = verify_token(token, SECRET, ALGO)
    assert result["valid"]
    assert result["payload"]["device_id"] == "sensor_01"


def test_expired_token_fails():
    token = create_token(_device_payload(), SECRET, ALGO, expiry_hours=-1)
    result = verify_token(token, SECRET, ALGO)
    assert not result["valid"]
    assert result["reason"] == "Token expired"


def test_wrong_secret_fails():
    token = create_token(_device_payload(), SECRET, ALGO)
    result = verify_token(token, "wrong-secret-intentionally-bad-xyz", ALGO)
    assert not result["valid"]
    assert result["reason"] == "Invalid token"


def test_verify_researcher_token_accepts_researcher():
    token = create_token(_researcher_payload(), SECRET, ALGO)
    result = verify_researcher_token(token, SECRET, ALGO)
    assert result["valid"]
    assert result["payload"]["username"] == "dr_smith"


def test_verify_researcher_token_rejects_device():
    token = create_token(_device_payload(), SECRET, ALGO)
    result = verify_researcher_token(token, SECRET, ALGO)
    assert not result["valid"]
    assert "researcher" in result["reason"]
