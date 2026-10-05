"""批量写入预演 + 确认（复核员提交前预演）集成测试。"""

from __future__ import annotations

import os
import tempfile

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(monkeypatch):
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    monkeypatch.setenv("QSO_DB_PATH", tmp.name)
    from app.main import app

    with TestClient(app) as c:
        yield c
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(tmp.name + suffix)
        except OSError:
            pass


def _log(no, station, counterpart, t, band="40m", mode="SSB"):
    return {
        "record_no": no,
        "station": station,
        "counterpart": counterpart,
        "time": t,
        "band": band,
        "mode": mode,
    }


def _make_drill(client, drill="PV", allowed=10):
    r = client.post(
        "/drills",
        json={"drill_id": drill, "allowed_time_diff_seconds": allowed},
    )
    assert r.status_code == 201, r.text


def _preview(client, drill, logs):
    return client.post(f"/drills/{drill}/logs/preview", json={"logs": logs})


def _confirm(client, drill, logs, base_version, token):
    return client.post(
        f"/drills/{drill}/logs/confirm",
        json={"logs": logs, "base_version": base_version, "token": token},
    )


# --------------------------------------------------------------------------
# 预演输出
# --------------------------------------------------------------------------
def test_preview_reports_current_proposed_and_diff(client):
    _make_drill(client)
    logs = [
        _log("r1", "BH1AAA", "BH2BBB", "2026-10-04T08:00:00+08:00"),
        _log("r2", "BH2BBB", "BH1AAA", "2026-10-04T00:00:05+00:00"),
        _log("r3", "BH3CCC", "BH4DDD", "2026-10-04T00:00:00+00:00"),
    ]
    r = _preview(client, "PV", logs)
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["drill_id"] == "PV"
    assert body["base_version"] == 0
    assert body["proposed_version"] == 1
    assert body["would_advance_version"] is True

    cur = body["current_result"]
    assert cur["version"] == 0 and cur["pairs"] == [] and cur["unpaired"] == []

    proposed = body["proposed_result"]
    assert proposed["version"] == 1
    assert len(proposed["pairs"]) == 1
    p = proposed["pairs"][0]
    assert (p["record_a"], p["record_b"]) == ("r1", "r2")
    assert p["time_diff_seconds"] == 5
    assert proposed["unpaired"] == ["r3"]
    assert proposed["total_time_diff_seconds"] == 5
    assert proposed["recomputed_at"] is None

    d = body["diff"]
    added = d["pairs"]["added"]
    assert len(added) == 1
    assert (added[0]["record_a"], added[0]["record_b"]) == ("r1", "r2")
    assert d["pairs"]["removed"] == []
    assert d["pairs"]["time_diff_changed"] == []
    assert d["unpaired"]["added"] == ["r3"]
    assert d["unpaired"]["removed"] == []
    assert d["total_time_diff_seconds"] == {"from": 0.0, "to": 5.0, "delta": 5.0}

    assert body["token"].startswith("pv1.")
    # 规范化批次：呼号/波段/模式大写，时间统一 UTC。
    nb = body["normalized_batch"]
    assert nb[0]["station"] == "BH1AAA"
    assert nb[0]["time"] == "2026-10-04T00:00:00+00:00"


def test_preview_is_readonly_no_state_change(client):
    _make_drill(client)
    logs = [_log("r1", "A", "B", "2026-10-04T00:00:00+00:00")]
    r = _preview(client, "PV", logs)
    assert r.status_code == 200

    assert client.get("/drills/PV/logs").json() == []
    result = client.get("/drills/PV/result").json()
    assert result["version"] == 0 and result["pairs"] == []
    # 无新快照。
    assert client.get("/drills/PV/snapshots").json()["versions"] == [0]
    # 可反复预演，标识一致（同基准版本、同规范化批次）。
    r2 = _preview(
        client,
        "PV",
        [{**logs[0], "station": " a ", "band": "40m"}],
    )
    assert r2.json()["token"] == r.json()["token"]


def test_preview_shows_pair_removal_and_time_diff_change(client):
    _make_drill(client, allowed=50)
    # 既有：r1-r2 时差 0。
    client.post("/drills/PV/logs", json={"logs": [
        _log("r1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("r2", "B", "A", "2026-10-04T00:00:00+00:00"),
        _log("r3", "B", "A", "2026-10-04T00:00:09+00:00"),
    ]})
    # 预演：把 r2 改到 +9 秒（仍可配对，时差 0 -> 9），并新增无互指的 r4。
    r = _preview(client, "PV", [
        _log("r2", "B", "A", "2026-10-04T00:00:09+00:00"),
        _log("r4", "Z9", "Z8", "2026-10-04T00:00:00+00:00"),
    ])
    assert r.status_code == 200, r.text
    d = r.json()["diff"]
    # r1-r2 仍配对但时差变化；r3 因全场最优与 r1 配对（时差同为 9 时
    # 字典序取 (r1,r2)），此处仅校验同对时差变化存在。
    tdc = d["pairs"]["time_diff_changed"]
    assert any(
        (x["record_a"], x["record_b"]) == ("r1", "r2")
        and x["from_time_diff_seconds"] == 0
        and x["to_time_diff_seconds"] == 9
        for x in tdc
    )
    assert "r4" in d["unpaired"]["added"]
    total = d["total_time_diff_seconds"]
    assert total["to"] - total["from"] == total["delta"]


# --------------------------------------------------------------------------
# 确认提交
# --------------------------------------------------------------------------
def test_confirm_commits_atomically_and_matches_preview(client):
    _make_drill(client)
    logs = [
        _log("r1", "BH1AAA", "BH2BBB", "2026-10-04T08:00:00+08:00"),
        _log("r2", "BH2BBB", "BH1AAA", "2026-10-04T00:00:05+00:00"),
        _log("r3", "BH3CCC", "BH4DDD", "2026-10-04T00:00:00+00:00"),
    ]
    pv = _preview(client, "PV", logs).json()

    r = _confirm(client, "PV", logs, pv["base_version"], pv["token"])
    assert r.status_code == 200, r.text
    result = r.json()
    assert result["version"] == 1
    assert result == client.get("/drills/PV/result").json()
    # 确认结果与预演的拟写入结果一致（recomputed_at 现态有值，其余一致）。
    proposed = pv["proposed_result"]
    for key in ("drill_id", "version", "sealed", "pairs", "unpaired",
                "total_time_diff_seconds"):
        assert result[key] == proposed[key]
    assert len(client.get("/drills/PV/logs").json()) == 3
    # 真正推进版本：留存 1 版快照。
    assert client.get("/drills/PV/snapshots").json()["versions"] == [0, 1]


def test_confirm_retransmit_does_not_advance_version(client):
    _make_drill(client)
    logs = [_log("r1", "A", "B", "2026-10-04T00:00:00+00:00")]
    pv = _preview(client, "PV", logs).json()
    assert pv["would_advance_version"] is True
    _confirm(client, "PV", logs, pv["base_version"], pv["token"])

    # 同内容（大小写/空白规范化后一致）再次预演：不会推进版本、差异全空。
    same = [{**logs[0], "station": " a ", "counterpart": "b", "band": "40M"}]
    pv2 = _preview(client, "PV", same).json()
    assert pv2["base_version"] == 1
    assert pv2["proposed_version"] == 1
    assert pv2["would_advance_version"] is False
    d = pv2["diff"]
    assert d["pairs"] == {"added": [], "removed": [], "time_diff_changed": []}
    assert d["unpaired"] == {"added": [], "removed": []}
    assert d["total_time_diff_seconds"]["delta"] == 0.0

    r = _confirm(client, "PV", same, pv2["base_version"], pv2["token"])
    assert r.status_code == 200 and r.json()["version"] == 1
    assert client.get("/drills/PV/snapshots").json()["versions"] == [0, 1]


def test_confirm_tampered_batch_rejected_token_mismatch(client):
    _make_drill(client)
    logs = [_log("r1", "A", "B", "2026-10-04T00:00:00+00:00")]
    pv = _preview(client, "PV", logs).json()

    tampered = [_log("r1", "A", "B", "2026-10-04T00:00:01+00:00")]
    r = _confirm(client, "PV", tampered, pv["base_version"], pv["token"])
    assert r.status_code == 409
    assert r.json()["error"] == "preview_token_mismatch"
    # 状态不变。
    assert client.get("/drills/PV/logs").json() == []
    assert client.get("/drills/PV/result").json()["version"] == 0

    # 伪造标识同样拒绝。
    r = _confirm(client, "PV", logs, pv["base_version"], "pv1.aa.bb")
    assert r.status_code == 409
    assert r.json()["error"] == "preview_token_mismatch"


def test_confirm_after_version_changed_rejected(client):
    _make_drill(client)
    logs = [_log("r1", "A", "B", "2026-10-04T00:00:00+00:00")]
    pv = _preview(client, "PV", logs).json()

    # 预演之后、确认之前，另一路写入推进了版本。
    other = client.put(
        "/drills/PV/logs/r9",
        json=_log("r9", "X", "Y", "2026-10-04T00:00:00+00:00"),
    )
    assert other.status_code == 200

    r = _confirm(client, "PV", logs, pv["base_version"], pv["token"])
    assert r.status_code == 409
    assert r.json()["error"] == "preview_base_version_changed"
    # r1 未被写入，r9 保留，版本停在 1。
    assert client.get("/drills/PV/result").json()["version"] == 1
    assert {x["record_no"] for x in client.get("/drills/PV/logs").json()} == {"r9"}

    # 以新版本重新预演并确认成功。
    pv2 = _preview(client, "PV", logs).json()
    assert pv2["base_version"] == 1
    r = _confirm(client, "PV", logs, pv2["base_version"], pv2["token"])
    assert r.status_code == 200 and r.json()["version"] == 2


def test_token_is_bound_to_drill(client):
    _make_drill(client, drill="D1")
    _make_drill(client, drill="D2")
    logs = [_log("r1", "A", "B", "2026-10-04T00:00:00+00:00")]
    pv = _preview(client, "D1", logs).json()
    # 用 D1 的标识去确认 D2：标识不匹配，拒绝。
    r = _confirm(client, "D2", logs, pv["base_version"], pv["token"])
    assert r.status_code == 409
    assert r.json()["error"] == "preview_token_mismatch"
    assert client.get("/drills/D2/logs").json() == []


# --------------------------------------------------------------------------
# 校验：非法记录 / 批内重复 / 锁定失效 / 封存 / 未知演练
# --------------------------------------------------------------------------
def test_preview_invalid_record_entire_batch_error(client):
    _make_drill(client)
    r = _preview(client, "PV", [
        _log("r1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("r2", "A", "A", "2026-10-04T00:00:01+00:00"),
    ])
    assert r.status_code == 422
    assert client.get("/drills/PV/logs").json() == []

    # 时间缺时区同样 422。
    r = _preview(client, "PV", [_log("r1", "A", "B", "2026-10-04T08:00:00")])
    assert r.status_code == 422


def test_preview_and_confirm_duplicate_record_in_batch(client):
    _make_drill(client)
    logs = [
        _log("r1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("r1", "A", "B", "2026-10-04T00:00:01+00:00"),
    ]
    r = _preview(client, "PV", logs)
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_record_in_batch"

    # 手工构造确认（无法通过预演拿到标识）也按整批规则拒绝。
    r = _confirm(client, "PV", logs, 0, "pv1.xx.yy")
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_record_in_batch"
    assert client.get("/drills/PV/logs").json() == []


def _seed_four(client, drill="PV", allowed=50):
    _make_drill(client, drill=drill, allowed=allowed)
    client.post(f"/drills/{drill}/logs", json={"logs": [
        _log("L1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("L2", "A", "B", "2026-10-04T00:01:40+00:00"),
        _log("R1", "B", "A", "2026-10-04T00:00:00+00:00"),
        _log("R2", "B", "A", "2026-10-04T00:00:50+00:00"),
    ]})


def test_preview_lock_invalidated_rejected_and_state_unchanged(client):
    _seed_four(client)
    client.put("/drills/PV/locks", json={"record_a": "L1", "record_b": "R1"})
    v_before = client.get("/drills/PV/result").json()["version"]

    # 预演把 R1 改到 +60 秒（>50）：锁定失效，整批报错。
    r = _preview(client, "PV", [
        _log("R1", "B", "A", "2026-10-04T00:01:00+00:00"),
    ])
    assert r.status_code == 409 and r.json()["error"] == "lock_invalidated"
    got = client.get("/drills/PV/result").json()
    assert got["version"] == v_before
    pairs = sorted((p["record_a"], p["record_b"]) for p in got["pairs"])
    assert pairs == [("L1", "R1"), ("L2", "R2")]

    # 合法更正（40 秒）预演成功，且锁定对强制出现在拟写入结果中。
    r = _preview(client, "PV", [
        _log("R1", "B", "A", "2026-10-04T00:00:40+00:00"),
    ])
    assert r.status_code == 200, r.text
    pairs = [
        (p["record_a"], p["record_b"])
        for p in r.json()["proposed_result"]["pairs"]
    ]
    assert ("L1", "R1") in pairs


def test_preview_and_confirm_on_sealed_drill_rejected(client):
    _seed_four(client)
    client.post("/drills/PV/seal")
    logs = [_log("L1", "A", "B", "2026-10-04T00:00:00+00:00")]

    r = _preview(client, "PV", logs)
    assert r.status_code == 409 and r.json()["error"] == "drill_sealed"

    r = _confirm(client, "PV", logs, 1, "pv1.xx.yy")
    assert r.status_code == 409 and r.json()["error"] == "drill_sealed"
    # 封存后版本/结果不变。
    assert client.get("/drills/PV/result").json()["version"] == 1


def test_preview_unknown_drill_is_404(client):
    logs = [_log("r1", "A", "B", "2026-10-04T00:00:00+00:00")]
    r = _preview(client, "NOPE", logs)
    assert r.status_code == 404 and r.json()["error"] == "drill_not_found"
    r = _confirm(client, "NOPE", logs, 0, "pv1.xx.yy")
    assert r.status_code == 404 and r.json()["error"] == "drill_not_found"


def test_token_survives_init_db_rerun(client):
    """HMAC 密钥随库持久化：重复初始化后既有演练标识仍可确认。"""
    from app import database

    _make_drill(client)
    logs = [_log("r1", "A", "B", "2026-10-04T00:00:00+00:00")]
    pv = _preview(client, "PV", logs).json()
    database.init_db()  # 幂等：密钥不更换
    r = _confirm(client, "PV", logs, pv["base_version"], pv["token"])
    assert r.status_code == 200 and r.json()["version"] == 1
