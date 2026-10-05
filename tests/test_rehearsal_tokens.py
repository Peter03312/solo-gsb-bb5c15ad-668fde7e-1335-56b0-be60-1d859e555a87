"""预演校验标识（HMAC token）纯函数单测。"""

from __future__ import annotations

from app import rehearsal


def _items():
    return [
        {"record_no": "r1", "station": "A", "counterpart": "B",
         "ts_micros": 1000, "band": "40M", "mode": "SSB"},
        {"record_no": "r2", "station": "B", "counterpart": "A",
         "ts_micros": 6000, "band": "40M", "mode": "SSB"},
    ]


def test_token_roundtrip_verifies():
    secret = "s3cr3t"
    token = rehearsal.make_token(secret, "D", 3, _items())
    assert token.startswith(rehearsal.TOKEN_PREFIX + ".")
    assert rehearsal.verify_token(secret, token, "D", 3, _items())


def test_token_rejects_wrong_drill_version_batch_and_secret():
    secret = "s3cr3t"
    items = _items()
    token = rehearsal.make_token(secret, "D", 3, items)

    assert not rehearsal.verify_token(secret, token, "OTHER", 3, items)
    assert not rehearsal.verify_token(secret, token, "D", 4, items)

    changed = [dict(items[0], ts_micros=9999), items[1]]
    assert not rehearsal.verify_token(secret, token, "D", 3, changed)

    reordered = [items[1], items[0]]
    assert not rehearsal.verify_token(secret, token, "D", 3, reordered)

    assert not rehearsal.verify_token("other-secret", token, "D", 3, items)
    assert not rehearsal.verify_token(secret, "forged", "D", 3, items)
    assert not rehearsal.verify_token(secret, token + "x", "D", 3, items)


def test_token_is_deterministic_and_canonical_serialization():
    secret = "k"
    t1 = rehearsal.make_token(secret, "D", 0, _items())
    t2 = rehearsal.make_token(secret, "D", 0, _items())
    assert t1 == t2
    # 规范化 JSON 键序无关、无空白
    raw = rehearsal.canonical_batch_json(_items())
    assert raw == rehearsal.canonical_batch_json(_items())
    assert b" " not in raw
