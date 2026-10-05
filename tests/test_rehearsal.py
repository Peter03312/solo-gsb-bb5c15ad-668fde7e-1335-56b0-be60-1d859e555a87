"""批量写入预演 / 确认提交（复核员提交前预演）集成测试。"""

from __future__ import annotations

import os
import tempfile
import threading

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


def _rehearse(client, drill, logs):
    return client.post(f"/drills/{drill}/logs/rehearse", json={"logs": logs})


def _commit(client, drill, logs, base_version, token):
    return client.post(
        f"/drills/{drill}/logs/commit",
        json={
            "logs": logs,
            "base_version": base_version,
            "token": token,
        },
    )


def _seed(client, drill="RH", allowed=50):
    client.post(
        "/drills",
        json={"drill_id": drill, "allowed_time_diff_seconds": allowed},
    )
    client.post(f"/drills/{drill}/logs", json={"logs": [
        _log("L1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("L2", "A", "B", "2026-10-04T00:01:40+00:00"),
        _log("R1", "B", "A", "2026-10-04T00:00:00+00:00"),
        _log("R2", "B", "A", "2026-10-04T00:00:50+00:00"),
    ]})


# --------------------------------------------------------------------------
# 预演本身：只读、结果正确
# --------------------------------------------------------------------------
def test_rehearse_previews_result_and_diff_without_state_change(client):
    _seed(client)
    snapshots_before = client.get("/drills/RH/snapshots").json()
    result_before = client.get("/drills/RH/result").json()

    batch = [
        _log("X1", "C", "D", "2026-10-04T00:00:00+00:00"),
        _log("X2", "D", "C", "2026-10-04T00:00:07+00:00"),
        _log("X3", "E", "F", "2026-10-04T00:00:00+00:00"),
    ]
    r = _rehearse(client, "RH", batch)
    assert r.status_code == 200, r.text
    body = r.json()

    # 绑定信息
    assert body["drill_id"] == "RH"
    assert body["base_version"] == 1
    assert body["sealed"] is False
    assert body["would_change_version"] is True
    assert body["token"].startswith("rehearse1.")

    # 当前结果
    cur = body["current_result"]
    assert cur["version"] == 1
    assert sorted((p["record_a"], p["record_b"]) for p in cur["pairs"]) == [
        ("L1", "R1"), ("L2", "R2")
    ]

    # 拟写入结果（不落库，无 recomputed_at）
    prop = body["proposed_result"]
    assert prop["version"] == 2
    assert prop["recomputed_at"] is None
    prop_pairs = sorted((p["record_a"], p["record_b"]) for p in prop["pairs"])
    assert prop_pairs == [("L1", "R1"), ("L2", "R2"), ("X1", "X2")]
    assert prop["unpaired"] == ["X3"]

    # 差异节
    changes = body["changes"]
    added = sorted((p["record_a"], p["record_b"]) for p in changes["pairs"]["added"])
    assert added == [("X1", "X2")]
    assert changes["pairs"]["removed"] == []
    assert changes["pairs"]["time_diff_changed"] == []
    assert changes["unpaired"] == {"added": ["X3"], "removed": []}
    assert changes["total_time_diff_seconds"] == {"from": 50.0, "to": 57.0, "delta": 7.0}

    # 回显规范化批次（大写/去空白后的终态）
    norm = body["normalized_logs"]
    assert [n["record_no"] for n in norm] == ["X1", "X2", "X3"]
    assert norm[0]["station"] == "C"
    assert norm[0]["time"].endswith("+00:00")

    # 预演不改变任何状态
    assert client.get("/drills/RH/result").json() == result_before
    assert client.get("/drills/RH/snapshots").json() == snapshots_before
    assert client.get("/drills/RH/logs").json().__len__() == 4
    assert client.get("/drills/RH/locks").json()["locks"] == []


def test_rehearse_on_empty_drill_previews_new_pairs(client):
    client.post(
        "/drills", json={"drill_id": "E", "allowed_time_diff_seconds": 10}
    )
    r = _rehearse(client, "E", [
        _log("1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("2", "B", "A", "2026-10-04T00:00:04+00:00"),
    ])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["base_version"] == 0
    assert body["current_result"]["pairs"] == []
    prop = body["proposed_result"]
    assert prop["version"] == 1
    assert [(p["record_a"], p["record_b"]) for p in prop["pairs"]] == [("1", "2")]
    # 空场 -> 有配对，未配对记录移除为空。
    assert body["changes"]["unpaired"]["removed"] == []


def test_rehearse_shows_pair_added_removed_and_time_diff_change(client):
    _seed(client)  # v1: (L1,R1)=0, (L2,R2)=50
    # 更正 R2 到 t0：最优解变为 (L1,R1)=0,(L2?,R2?) — 构造时差变化+增删：
    # 锁定 (L1,R1) 后，把 R1 更正到 t+3 -> 同对时差 0->3，其余不变。
    assert client.put(
        "/drills/RH/locks", json={"record_a": "L1", "record_b": "R1"}
    ).status_code == 200

    r = _rehearse(client, "RH", [
        _log("R1", "B", "A", "2026-10-04T00:00:03+00:00"),
    ])
    assert r.status_code == 200, r.text
    changes = r.json()["changes"]
    assert changes["pairs"]["added"] == []
    assert changes["pairs"]["removed"] == []
    assert changes["pairs"]["time_diff_changed"] == [{
        "record_a": "L1",
        "record_b": "R1",
        "from_time_diff_seconds": 0.0,
        "to_time_diff_seconds": 3.0,
    }]
    assert changes["unpaired"] == {"added": [], "removed": []}
    assert changes["total_time_diff_seconds"] == {"from": 50.0, "to": 53.0, "delta": 3.0}


def test_rehearse_same_content_retransmit_does_not_change_version(client):
    _seed(client)
    # 与现存 R1 规范化后完全相同（大小写/空白差异；R1 为 B->A）
    same = [_log("R1", " b ", " a ", "2026-10-04T08:00:00+08:00", band="40m")]
    r = _rehearse(client, "RH", same)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["would_change_version"] is False
    assert body["proposed_result"]["version"] == body["base_version"] == 1
    assert body["changes"]["pairs"] == {
        "added": [], "removed": [], "time_diff_changed": []
    }
    assert body["changes"]["unpaired"] == {"added": [], "removed": []}
    assert body["changes"]["total_time_diff_seconds"]["delta"] == 0.0
    # 预演不落库
    assert client.get("/drills/RH/result").json()["version"] == 1


def test_rehearse_invalid_record_is_422_and_entire_batch_rejected(client):
    _seed(client)
    r = _rehearse(client, "RH", [
        _log("X1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("X2", "A", "A", "2026-10-04T00:00:01+00:00"),  # 同呼号
    ])
    assert r.status_code == 422
    assert "不能相同" in r.text
    # naive 时间同样 422
    r = _rehearse(client, "RH", [_log("X9", "A", "B", "2026-10-04T00:00:00")])
    assert r.status_code == 422 and "时区" in r.text
    # 空批次 422
    r = client.post("/drills/RH/logs/rehearse", json={"logs": []})
    assert r.status_code == 422
    assert client.get("/drills/RH/result").json()["version"] == 1


def test_rehearse_duplicate_record_in_batch_is_409(client):
    _seed(client)
    r = _rehearse(client, "RH", [
        _log("X1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("X1", "A", "B", "2026-10-04T00:00:01+00:00"),
    ])
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_record_in_batch"
    assert client.get("/drills/RH/result").json()["version"] == 1


def test_rehearse_lock_invalidated_is_409_and_state_unchanged(client):
    _seed(client)
    client.put("/drills/RH/locks", json={"record_a": "L1", "record_b": "R1"})
    v = client.get("/drills/RH/result").json()["version"]

    # 把 R1 更正到 60 秒差（>50）-> 锁定失效，预演整批报错
    r = _rehearse(client, "RH", [
        _log("R1", "B", "A", "2026-10-04T00:01:00+00:00"),
    ])
    assert r.status_code == 409
    assert r.json()["error"] == "lock_invalidated"
    assert client.get("/drills/RH/result").json()["version"] == v
    # 锁定仍在
    locks = client.get("/drills/RH/locks").json()["locks"]
    assert [(x["record_a"], x["record_b"]) for x in locks] == [("L1", "R1")]


def test_rehearse_sealed_drill_is_409(client):
    _seed(client)
    client.post("/drills/RH/seal")
    r = _rehearse(client, "RH", [
        _log("X1", "A", "B", "2026-10-04T00:00:00+00:00")
    ])
    assert r.status_code == 409 and r.json()["error"] == "drill_sealed"


def test_rehearse_unknown_drill_is_404(client):
    r = _rehearse(client, "NOPE", [
        _log("1", "A", "B", "2026-10-04T00:00:00+00:00")
    ])
    assert r.status_code == 404 and r.json()["error"] == "drill_not_found"


# --------------------------------------------------------------------------
# 确认提交：标识/版本/封存校验 + 与批量写入同语义原子落库
# --------------------------------------------------------------------------
def test_commit_after_rehearse_applies_batch_atomically(client):
    _seed(client)
    batch = [
        _log("X1", "C", "D", "2026-10-04T00:00:00+00:00"),
        _log("X2", "D", "C", "2026-10-04T00:00:07+00:00"),
    ]
    pre = _rehearse(client, "RH", batch).json()

    r = _commit(client, "RH", batch, pre["base_version"], pre["token"])
    assert r.status_code == 200, r.text
    result = r.json()
    # 响应结构与 POST /logs 完全一致
    assert set(result) == {
        "drill_id", "version", "sealed", "pairs", "unpaired",
        "total_time_diff_seconds", "recomputed_at",
    }
    assert result["version"] == 2
    prop_pairs = sorted(
        (p["record_a"], p["record_b"]) for p in pre["proposed_result"]["pairs"]
    )
    got_pairs = sorted((p["record_a"], p["record_b"]) for p in result["pairs"])
    assert got_pairs == prop_pairs
    # 版本推进同事务留存快照
    assert client.get("/drills/RH/snapshots/2").status_code == 200


def test_commit_retransmit_does_not_bump_version(client):
    _seed(client)
    same = [_log("R1", " b ", " a ", "2026-10-04T00:00:00+00:00")]
    pre = _rehearse(client, "RH", same).json()
    assert pre["would_change_version"] is False
    r = _commit(client, "RH", same, pre["base_version"], pre["token"])
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 1
    # 不留新快照
    assert client.get("/drills/RH/snapshots").json()["versions"] == [0, 1]


def test_commit_with_wrong_token_is_409_and_state_unchanged(client):
    _seed(client)
    batch = [_log("X1", "C", "D", "2026-10-04T00:00:00+00:00")]
    pre = _rehearse(client, "RH", batch).json()

    r = _commit(client, "RH", batch, pre["base_version"], "rehearse1.deadbeef")
    assert r.status_code == 409
    assert r.json()["error"] == "rehearsal_token_mismatch"
    assert client.get("/drills/RH/result").json()["version"] == 1
    assert len(client.get("/drills/RH/logs").json()) == 4

    # 篡改批次内容 -> 标识不再匹配（绑定规范化批次）
    tampered = [_log("X1", "C", "D", "2026-10-04T00:00:09+00:00")]
    r = _commit(client, "RH", tampered, pre["base_version"], pre["token"])
    assert r.status_code == 409
    assert r.json()["error"] == "rehearsal_token_mismatch"
    assert client.get("/drills/RH/result").json()["version"] == 1

    # 篡改基准版本 -> 标识不匹配
    r = _commit(client, "RH", batch, 5, pre["token"])
    assert r.status_code == 409
    assert r.json()["error"] == "rehearsal_token_mismatch"


def test_commit_after_version_changed_is_409_conflict(client):
    _seed(client)
    batch = [_log("X1", "C", "D", "2026-10-04T00:00:00+00:00")]
    pre = _rehearse(client, "RH", batch).json()

    # 预演之后他人先推进了版本
    client.put(
        "/drills/RH/logs/Y1",
        json=_log("Y1", "G", "H", "2026-10-04T00:00:00+00:00"),
    )
    r = _commit(client, "RH", batch, pre["base_version"], pre["token"])
    assert r.status_code == 409
    assert r.json()["error"] == "rehearsal_version_conflict"
    # 状态停在他人写入后，预演批次未应用
    result = client.get("/drills/RH/result").json()
    assert result["version"] == 2
    assert [x["record_no"] for x in client.get("/drills/RH/logs").json()
            if x["record_no"] == "X1"] == []

    # 重新预演后可用新标识提交
    pre2 = _rehearse(client, "RH", batch).json()
    assert pre2["base_version"] == 2
    r = _commit(client, "RH", batch, 2, pre2["token"])
    assert r.status_code == 200 and r.json()["version"] == 3


def test_commit_after_seal_is_409(client):
    _seed(client)
    batch = [_log("X1", "C", "D", "2026-10-04T00:00:00+00:00")]
    pre = _rehearse(client, "RH", batch).json()
    # 预演后演练被封存（版本号不变，但状态变化）
    client.post("/drills/RH/seal")
    r = _commit(client, "RH", batch, pre["base_version"], pre["token"])
    assert r.status_code == 409 and r.json()["error"] == "drill_sealed"
    assert len(client.get("/drills/RH/logs").json()) == 4
    # 封存后预演本身也被拒绝
    assert _rehearse(client, "RH", batch).status_code == 409


def test_commit_unknown_drill_is_404(client):
    batch = [_log("1", "A", "B", "2026-10-04T00:00:00+00:00")]
    r = _commit(client, "NOPE", batch, 0, "rehearse1.x")
    assert r.status_code == 404 and r.json()["error"] == "drill_not_found"


def test_commit_duplicate_and_invalid_batch_rejected(client):
    _seed(client)
    # 批内重复在标识校验之前即整批拒绝
    r = client.post("/drills/RH/logs/commit", json={"logs": [
        _log("X1", "A", "B", "2026-10-04T00:00:00+00:00"),
        _log("X1", "A", "B", "2026-10-04T00:00:01+00:00"),
    ], "base_version": 1, "token": "rehearse1.x"})
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_record_in_batch"

    # 非法记录 422（路由层）
    r = client.post("/drills/RH/logs/commit", json={"logs": [
        _log("X2", "A", "A", "2026-10-04T00:00:00+00:00"),
    ], "base_version": 1, "token": "rehearse1.x"})
    assert r.status_code == 422

    # 负基准版本 422
    r = client.post("/drills/RH/logs/commit", json={"logs": [
        _log("X3", "A", "B", "2026-10-04T00:00:00+00:00"),
    ], "base_version": -1, "token": "rehearse1.x"})
    assert r.status_code == 422
    assert client.get("/drills/RH/result").json()["version"] == 1


def test_commit_lock_invalidated_race_rejected(client):
    """预演时锁定有效，提交时锁定已被他人改失效 -> 落库阶段整批拒绝。"""
    _seed(client)
    client.put("/drills/RH/locks", json={"record_a": "L1", "record_b": "R1"})
    base = client.get("/drills/RH/result").json()["version"]

    # 预演一个本身不触碰锁定的合规批次
    batch = [_log("X1", "C", "D", "2026-10-04T00:00:00+00:00")]
    pre = _rehearse(client, "RH", batch).json()
    assert pre["base_version"] == base

    # 他人先写入一条新记录推进版本（与预演批次无关，锁定仍有效）
    client.put(
        "/drills/RH/logs/Z1",
        json=_log("Z1", "G", "H", "2026-10-04T00:00:00+00:00"),
    )
    r = _commit(client, "RH", batch, base, pre["token"])
    assert r.status_code == 409
    assert r.json()["error"] == "rehearsal_version_conflict"


def test_token_distinct_across_drill_version_and_batch(client):
    _seed(client)
    b1 = [_log("X1", "C", "D", "2026-10-04T00:00:00+00:00")]
    b2 = [_log("X1", "C", "D", "2026-10-04T00:00:01+00:00")]
    t1 = _rehearse(client, "RH", b1).json()["token"]
    t2 = _rehearse(client, "RH", b2).json()["token"]
    assert t1 != t2

    client.post(
        "/drills", json={"drill_id": "OT", "allowed_time_diff_seconds": 50}
    )
    t_other = _rehearse(client, "OT", b1).json()["token"]
    assert t_other != t1

    # 同一批次重复预演：基准版本未变 -> 标识稳定（幂等可重算）
    assert _rehearse(client, "RH", b1).json()["token"] == t1


def test_concurrent_commits_second_gets_version_conflict(client):
    _seed(client)
    batch = [_log("X1", "C", "D", "2026-10-04T00:00:00+00:00")]
    pre = _rehearse(client, "RH", batch).json()

    outcomes = []

    def commit():
        r = _commit(client, "RH", batch, pre["base_version"], pre["token"])
        outcomes.append(r.status_code)

    t1 = threading.Thread(target=commit)
    t2 = threading.Thread(target=commit)
    t1.start(); t2.start()
    t1.join(); t2.join()
    assert sorted(outcomes) == [200, 409]
    assert client.get("/drills/RH/result").json()["version"] == 2
