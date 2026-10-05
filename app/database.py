"""SQLite 持久化层。

并发写入由 SQLite 的写事务串行化保证：每次写入都开启
``BEGIN IMMEDIATE`` 立即获取 RESERVED 锁，第二个并发写入会在获取锁时
阻塞/报忙；配合 busy_timeout 重试，所有写入被串行提交。版本号、
日志与配对结果在同一个事务中更新，保证原子性。

启用 WAL 模式以支持“封存/计算期间仍可并发查询最终结果”。
"""

from __future__ import annotations

import json
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Sequence

from . import rehearsal as rehearsal_tokens
from . import config
from .matching import (
    InvalidLockError,
    MatchResult,
    Pair,
    Record,
    match_records,
    pair_eligibility,
)

MICROS_PER_SECOND = 1_000_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS drills (
    drill_id           TEXT PRIMARY KEY,
    allowed_diff_secs  REAL NOT NULL CHECK (allowed_diff_secs >= 0),
    version            INTEGER NOT NULL DEFAULT 0,
    sealed             INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL,
    sealed_at          TEXT
);

CREATE TABLE IF NOT EXISTS logs (
    drill_id     TEXT NOT NULL,
    record_no    TEXT NOT NULL,
    station      TEXT NOT NULL,
    counterpart  TEXT NOT NULL,
    ts_micros    INTEGER NOT NULL,
    ts_iso       TEXT NOT NULL,
    band         TEXT NOT NULL,
    mode         TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (drill_id, record_no),
    FOREIGN KEY (drill_id) REFERENCES drills(drill_id)
);

CREATE TABLE IF NOT EXISTS results (
    drill_id                  TEXT PRIMARY KEY,
    version                   INTEGER NOT NULL,
    pairs_json                TEXT NOT NULL,
    unpaired_json             TEXT NOT NULL,
    total_time_diff_seconds   REAL NOT NULL,
    recomputed_at             TEXT NOT NULL,
    FOREIGN KEY (drill_id) REFERENCES drills(drill_id)
);

-- 复核员人工锁定（互证）：记录号内部升序，一条记录至多被一个锁定占用。
-- 占用冲突由服务层在写事务内校验（SQLite 不支持跨列互斥约束）。
CREATE TABLE IF NOT EXISTS locks (
    drill_id     TEXT NOT NULL,
    record_a     TEXT NOT NULL,
    record_b     TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (drill_id, record_a, record_b),
    FOREIGN KEY (drill_id) REFERENCES drills(drill_id)
);

-- 版本快照（复核员按版本回看/比较）：版本真正推进时与版本、结果、锁定
-- 同事务留存该版的配对、未配对记录、总时差与锁定清单，之后不再改变。
-- v0 在创建演练时留存；重传、失败回滚、封存不产生新快照。
CREATE TABLE IF NOT EXISTS snapshots (
    drill_id                  TEXT NOT NULL,
    version                   INTEGER NOT NULL,
    pairs_json                TEXT NOT NULL,
    unpaired_json             TEXT NOT NULL,
    total_time_diff_seconds   REAL NOT NULL,
    locks_json                TEXT NOT NULL,
    created_at                 TEXT NOT NULL,
    PRIMARY KEY (drill_id, version),
    FOREIGN KEY (drill_id) REFERENCES drills(drill_id)
);

-- 服务级元数据：批量写入预演校验标识（HMAC）的签名密钥。
-- 密钥只在本服务验签时使用，不随预演/批次落库；启动初始化时幂等生成。
CREATE TABLE IF NOT EXISTS service_meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
"""


# --------------------------------------------------------------------------
# 领域错误
# --------------------------------------------------------------------------
class ServiceError(Exception):
    """业务错误基类，附带 HTTP 状态码与机器可读错误码。"""

    status_code = 400
    error_code = "bad_request"

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class DrillNotFoundError(ServiceError):
    status_code = 404
    error_code = "drill_not_found"


class DrillExistsError(ServiceError):
    status_code = 409
    error_code = "drill_exists"


class SealedError(ServiceError):
    status_code = 409
    error_code = "drill_sealed"


class DuplicateRecordInBatchError(ServiceError):
    status_code = 409
    error_code = "duplicate_record_in_batch"


class LockNotFoundError(ServiceError):
    """解除的锁定不存在。"""

    status_code = 404
    error_code = "lock_not_found"


class LockSameRecordError(ServiceError):
    """锁定请求中两个记录号相同。"""

    status_code = 422
    error_code = "lock_same_record"


class LockRecordMissingError(ServiceError):
    """锁定引用了不存在的记录。"""

    status_code = 404
    error_code = "lock_record_missing"


class LockNotCandidateError(ServiceError):
    """两条现存日志不满足互证候选条件（互指/波段模式/时差）。"""

    status_code = 409
    error_code = "lock_not_candidate"


class LockOccupiedError(ServiceError):
    """锁定涉及的记录已被其他锁定占用。"""

    status_code = 409
    error_code = "record_already_locked"


class LockInvalidatedError(ServiceError):
    """写入/更正使既有锁定失效，整次写入被拒绝。"""

    status_code = 409
    error_code = "lock_invalidated"


class SnapshotNotFoundError(ServiceError):
    """指定版本未留存快照（升级前的旧版本不可用现态补造）。"""

    status_code = 404
    error_code = "snapshot_not_found"


class RehearsalTokenMismatchError(ServiceError):
    """确认提交的校验标识与演练/基准版本/规范化批次不匹配。"""

    status_code = 409
    error_code = "rehearsal_token_mismatch"


class RehearsalVersionConflictError(ServiceError):
    """预演后基准版本已变化（他人已推进版本或演练已封存）。"""

    status_code = 409
    error_code = "rehearsal_version_conflict"


# --------------------------------------------------------------------------
# 连接管理
# --------------------------------------------------------------------------
def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.database_url(),
        timeout=30.0,
        isolation_level=None,  # 手工控制事务（autocommit 模式）
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    """创建数据目录、表结构并开启 WAL（幂等）。"""
    import os

    db_path = config.database_url()
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = connect()
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.executescript(SCHEMA)
        _ensure_service_secret(conn)
        # 升级既有库：从升级时的当前版本开始留存快照；更早的版本不补造。
        _backfill_legacy_snapshots(conn)
    finally:
        conn.close()


_SERVICE_SECRET_KEY = "rehearsal_hmac_secret"


def _ensure_service_secret(conn: sqlite3.Connection) -> str:
    """幂等取得预演签名密钥：不存在则在独立写事务中生成留存。"""
    row = conn.execute(
        "SELECT value FROM service_meta WHERE key = ?",
        (_SERVICE_SECRET_KEY,),
    ).fetchone()
    if row is not None:
        return row["value"]
    secret = secrets.token_hex(32)
    with _write_tx(conn):
        conn.execute(
            "INSERT INTO service_meta (key, value) VALUES (?, ?)",
            (_SERVICE_SECRET_KEY, secret),
        )
    return secret


def _get_service_secret(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT value FROM service_meta WHERE key = ?",
        (_SERVICE_SECRET_KEY,),
    ).fetchone()
    if row is None:  # 理论上不会发生（启动初始化幂等生成）
        return _ensure_service_secret(conn)
    return row["value"]


@contextmanager
def _read_tx(conn: sqlite3.Connection):
    """只读事务（延迟事务，不取写锁）：保证整段预演读取同一快照。"""
    conn.execute("BEGIN")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _backfill_legacy_snapshots(conn: sqlite3.Connection) -> None:
    """为升级前已存在、尚无快照的演练留存“当前版本”快照。

    每个这样的演练仅在其当前版本留存一份（内容取自升级时的现态），
    之前版本不可用现态补造。幂等：已有任一快照的演练整体跳过。
    """
    rows = conn.execute(
        """
        SELECT d.drill_id AS drill_id
        FROM drills d
        WHERE NOT EXISTS (
            SELECT 1 FROM snapshots s WHERE s.drill_id = d.drill_id
        )
        """
    ).fetchall()
    if not rows:
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = _now_iso()
        for r in rows:
            drill_id = r["drill_id"]
            res = conn.execute(
                "SELECT * FROM results WHERE drill_id = ?", (drill_id,)
            ).fetchone()
            if res is None:
                # 理论上不会发生（建演练即写空结果）；防御性补一条空结果。
                pairs_json, unpaired_json, total = "[]", "[]", 0.0
                version = conn.execute(
                    "SELECT version FROM drills WHERE drill_id = ?",
                    (drill_id,),
                ).fetchone()["version"]
            else:
                pairs_json = res["pairs_json"]
                unpaired_json = res["unpaired_json"]
                total = res["total_time_diff_seconds"]
                version = res["version"]
            locks = _lock_items(conn, drill_id)
            conn.execute(
                "INSERT INTO snapshots (drill_id, version, pairs_json, "
                "unpaired_json, total_time_diff_seconds, locks_json, "
                "created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    drill_id, version, pairs_json, unpaired_json, total,
                    json.dumps(locks, ensure_ascii=False), now,
                ),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


@contextmanager
def _write_tx(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE 写事务：立即拿写锁，提交/回滚原子。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------
# 序列化辅助
# --------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _micros_to_iso(ts_micros: int) -> str:
    return datetime.fromtimestamp(
        ts_micros / MICROS_PER_SECOND, tz=timezone.utc
    ).isoformat()


def _record_from_row(row: sqlite3.Row) -> Record:
    return Record(
        record_no=row["record_no"],
        station=row["station"],
        counterpart=row["counterpart"],
        ts_micros=row["ts_micros"],
        band=row["band"],
        mode=row["mode"],
    )


def _load_records(conn: sqlite3.Connection, drill_id: str) -> list[Record]:
    rows = conn.execute(
        "SELECT * FROM logs WHERE drill_id = ? ORDER BY rowid", (drill_id,)
    ).fetchall()
    return [_record_from_row(r) for r in rows]


def _record_map(records: Iterable[Record]) -> dict[str, Record]:
    return {r.record_no: r for r in records}


def _load_lock_keys(conn: sqlite3.Connection, drill_id: str) -> list[tuple[str, str]]:
    rows = conn.execute(
        "SELECT record_a, record_b FROM locks WHERE drill_id = ? "
        "ORDER BY record_a, record_b",
        (drill_id,),
    ).fetchall()
    return [(r["record_a"], r["record_b"]) for r in rows]


def _check_lock_candidate(
    record_map: dict[str, Record], allowed_micros: int, a: str, b: str
) -> None:
    """把算法层原因码翻译为对应业务错误抛出；合法时静默返回。"""
    problem = pair_eligibility(record_map, allowed_micros, a, b)
    if problem is None:
        return
    code, message = problem
    if code == "same_record":
        raise LockSameRecordError(message)
    if code == "record_missing":
        raise LockRecordMissingError(message)
    raise LockNotCandidateError(message)


def _persist_result(
    conn: sqlite3.Connection, drill_id: str, version: int, result: MatchResult
) -> None:
    pairs_payload = [
        {
            "record_a": p.record_a,
            "record_b": p.record_b,
            "time_diff_seconds": p.time_diff_seconds,
        }
        for p in result.pairs
    ]
    conn.execute(
        """
        INSERT INTO results (drill_id, version, pairs_json, unpaired_json,
                             total_time_diff_seconds, recomputed_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(drill_id) DO UPDATE SET
            version = excluded.version,
            pairs_json = excluded.pairs_json,
            unpaired_json = excluded.unpaired_json,
            total_time_diff_seconds = excluded.total_time_diff_seconds,
            recomputed_at = excluded.recomputed_at
        """,
        (
            drill_id,
            version,
            json.dumps(pairs_payload, ensure_ascii=False),
            json.dumps(list(result.unpaired), ensure_ascii=False),
            result.total_time_diff_seconds,
            _now_iso(),
        ),
    )


def _persist_snapshot(
    conn: sqlite3.Connection,
    drill_id: str,
    version: int,
    result: MatchResult,
    locks: list[dict],
) -> None:
    """在写事务内留存版本快照（配对/未配对/总时差/锁定清单）。

    与版本推进、结果重算在同一事务提交；快照一旦留存不再改变。
    """
    pairs_payload = [
        {
            "record_a": p.record_a,
            "record_b": p.record_b,
            "time_diff_seconds": p.time_diff_seconds,
        }
        for p in result.pairs
    ]
    conn.execute(
        """
        INSERT INTO snapshots (drill_id, version, pairs_json, unpaired_json,
                               total_time_diff_seconds, locks_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            drill_id,
            version,
            json.dumps(pairs_payload, ensure_ascii=False),
            json.dumps(list(result.unpaired), ensure_ascii=False),
            result.total_time_diff_seconds,
            json.dumps(locks, ensure_ascii=False),
            _now_iso(),
        ),
    )


def _recompute(conn: sqlite3.Connection, drill_id: str) -> None:
    """重算全场配对：版本 +1（调用方负责），结果与版本同事务落库。

    先保留全部锁定对，再对剩余日志按“配对数最多、总时差最小、
    记录号字典序最小”求解；锁定即使降低全场配对数也不替换。
    """
    drill = _get_drill_row(conn, drill_id)
    version = drill["version"] + 1
    records = _load_records(conn, drill_id)
    lock_keys = _load_lock_keys(conn, drill_id)
    try:
        result = match_records(
            records, drill["allowed_diff_secs"], locked_keys=lock_keys
        )
    except InvalidLockError as exc:
        raise LockInvalidatedError(str(exc))
    conn.execute(
        "UPDATE drills SET version = ? WHERE drill_id = ?",
        (version, drill_id),
    )
    _persist_result(conn, drill_id, version, result)
    # 版本真正推进：同事务留存该版快照（含当时锁定清单）。
    rmap = _record_map(records)
    _persist_snapshot(
        conn, drill_id, version, result, _lock_items(conn, drill_id, rmap)
    )


def _get_drill_row(
    conn: sqlite3.Connection, drill_id: str
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM drills WHERE drill_id = ?", (drill_id,)
    ).fetchone()
    if row is None:
        raise DrillNotFoundError(f"演练号不存在: {drill_id}")
    return row


def _require_unsealed(row: sqlite3.Row) -> None:
    if row["sealed"]:
        raise SealedError(f"演练 {row['drill_id']} 已封存，拒绝写入")


# --------------------------------------------------------------------------
# 对外服务操作
# --------------------------------------------------------------------------
def create_drill(drill_id: str, allowed_diff_secs: float) -> dict:
    conn = connect()
    try:
        with _write_tx(conn):
            exists = conn.execute(
                "SELECT 1 FROM drills WHERE drill_id = ?", (drill_id,)
            ).fetchone()
            if exists is not None:
                raise DrillExistsError(f"演练号已存在: {drill_id}")
            conn.execute(
                "INSERT INTO drills (drill_id, allowed_diff_secs, version, "
                "sealed, created_at) VALUES (?, ?, 0, 0, ?)",
                (drill_id, allowed_diff_secs, _now_iso()),
            )
            # 空场结果：版本 0，无配对。
            empty = MatchResult((), (), 0.0)
            _persist_result(conn, drill_id, 0, empty)
            # 创建演练的 0 版即可回看：同事务留存空场快照。
            _persist_snapshot(conn, drill_id, 0, empty, [])
        return get_drill(drill_id)
    finally:
        conn.close()


def upsert_log(
    drill_id: str,
    record_no: str,
    station: str,
    counterpart: str,
    ts_micros: int,
    band: str,
    mode: str,
) -> dict:
    """写入/更正单条日志；同内容重传不推进版本，异内容更正后重算全场。"""
    conn = connect()
    try:
        changed = False
        with _write_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            row = conn.execute(
                "SELECT * FROM logs WHERE drill_id = ? AND record_no = ?",
                (drill_id, record_no),
            ).fetchone()
            ts_iso = _micros_to_iso(ts_micros)
            if row is None:
                conn.execute(
                    "INSERT INTO logs (drill_id, record_no, station, "
                    "counterpart, ts_micros, ts_iso, band, mode, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        drill_id, record_no, station, counterpart,
                        ts_micros, ts_iso, band, mode, _now_iso(),
                    ),
                )
                changed = True
            elif (
                row["station"], row["counterpart"], row["ts_micros"],
                row["band"], row["mode"],
            ) != (station, counterpart, ts_micros, band, mode):
                conn.execute(
                    "UPDATE logs SET station = ?, counterpart = ?, "
                    "ts_micros = ?, ts_iso = ?, band = ?, mode = ?, "
                    "updated_at = ? WHERE drill_id = ? AND record_no = ?",
                    (
                        station, counterpart, ts_micros, ts_iso,
                        band, mode, _now_iso(), drill_id, record_no,
                    ),
                )
                changed = True
            # 同内容重传：什么都不改，版本不推进。
            if changed:
                _recompute(conn, drill_id)
        return get_result(drill_id)
    finally:
        conn.close()


def _check_batch_unique(items: Sequence[dict]) -> None:
    """批次内同一记录号出现多次视为非法（无法判定终态）。"""
    seen: set[str] = set()
    for it in items:
        if it["record_no"] in seen:
            raise DuplicateRecordInBatchError(
                f"批次内记录号重复: {it['record_no']}"
            )
        seen.add(it["record_no"])


def _apply_batch_items(
    conn: sqlite3.Connection, drill_id: str, items: Sequence[dict]
) -> bool:
    """在写事务内按 upsert 语义应用规范化批次，返回是否有任何内容变化。

    与单条写入一致：记录不存在则插入；规范化后内容不同则更正；
    规范化后完全相同则视为重传（不动该行）。
    """
    changed = False
    for it in items:
        record_no = it["record_no"]
        ts_micros = it["ts_micros"]
        ts_iso = _micros_to_iso(ts_micros)
        row = conn.execute(
            "SELECT * FROM logs WHERE drill_id = ? AND record_no = ?",
            (drill_id, record_no),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO logs (drill_id, record_no, station, "
                "counterpart, ts_micros, ts_iso, band, mode, "
                "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    drill_id, record_no, it["station"],
                    it["counterpart"], ts_micros, ts_iso,
                    it["band"], it["mode"], _now_iso(),
                ),
            )
            changed = True
        elif (
            row["station"], row["counterpart"], row["ts_micros"],
            row["band"], row["mode"],
        ) != (
            it["station"], it["counterpart"], ts_micros,
            it["band"], it["mode"],
        ):
            conn.execute(
                "UPDATE logs SET station = ?, counterpart = ?, "
                "ts_micros = ?, ts_iso = ?, band = ?, mode = ?, "
                "updated_at = ? WHERE drill_id = ? AND record_no = ?",
                (
                    it["station"], it["counterpart"], ts_micros,
                    ts_iso, it["band"], it["mode"], _now_iso(),
                    drill_id, record_no,
                ),
            )
            changed = True
    return changed


def batch_upsert_logs(
    drill_id: str, items: Iterable[dict]
) -> dict:
    """批量写入/更正：整体一个事务，任一非法则整批拒绝。

    批次内同一记录号出现多次视为非法（无法判定终态）。
    """
    items = list(items)
    _check_batch_unique(items)

    conn = connect()
    try:
        with _write_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            changed = _apply_batch_items(conn, drill_id, items)
            if changed:
                _recompute(conn, drill_id)
        return get_result(drill_id)
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 批量写入预演（只读）与按标识确认提交：沿用现有规范化、整批校验、
# 锁定有效性与全场最优配对规则；预演不改变任何状态。
# --------------------------------------------------------------------------
def _merge_batch_items(
    records: Sequence[Record], items: Sequence[dict]
) -> tuple[list[Record], bool]:
    """在内存中按 upsert 语义把规范化批次应用到现存记录（不写库）。

    返回 ``(拟写入后的记录列表（保持现存写入顺序，新记录按批次顺序追加）,
    是否有任何内容变化)``，与 :func:`_apply_batch_items` 的判定逐字段一致。
    """
    by_no = {r.record_no: r for r in records}
    merged = list(records)
    changed = False
    for it in items:
        incoming = Record(
            record_no=it["record_no"],
            station=it["station"],
            counterpart=it["counterpart"],
            ts_micros=it["ts_micros"],
            band=it["band"],
            mode=it["mode"],
        )
        existing = by_no.get(it["record_no"])
        if existing is None:
            merged.append(incoming)
            by_no[incoming.record_no] = incoming
            changed = True
        elif existing != incoming:
            for i, r in enumerate(merged):
                if r.record_no == incoming.record_no:
                    merged[i] = incoming
                    break
            by_no[incoming.record_no] = incoming
            changed = True
    return merged, changed


def _proposed_result(
    drill_id: str,
    sealed: bool,
    base_version: int,
    result: MatchResult,
) -> dict:
    """把预演算出的拟写入结果组装为与 GET /result 同构的负载。"""
    return {
        "drill_id": drill_id,
        "version": base_version + 1,
        "sealed": sealed,
        "pairs": [
            {
                "record_a": p.record_a,
                "record_b": p.record_b,
                "time_diff_seconds": p.time_diff_seconds,
            }
            for p in result.pairs
        ],
        "unpaired": list(result.unpaired),
        "total_time_diff_seconds": result.total_time_diff_seconds,
        # 预演不重算落库，没有 recomputed_at 时间。
        "recomputed_at": None,
    }


def _normalized_log_items(items: Sequence[dict]) -> list[dict]:
    """回显服务端规范化后的批次（确认时原样回传即可）。"""
    return [
        {
            "record_no": it["record_no"],
            "station": it["station"],
            "counterpart": it["counterpart"],
            "ts_micros": it["ts_micros"],
            "time": _micros_to_iso(it["ts_micros"]),
            "band": it["band"],
            "mode": it["mode"],
        }
        for it in items
    ]


def rehearse_batch(drill_id: str, items: Iterable[dict]) -> dict:
    """预演批量写入：只读，不改变日志、锁定、结果、版本与快照。

    沿用现有规范化（调用方完成）、整批记录号去重、封存校验、锁定有效性
    校验与全场最优配对规则；非法记录由路由层 Pydantic 拦截（422），
    批内重复或锁定失效在此整批报错。
    """
    items = list(items)
    _check_batch_unique(items)

    conn = connect()
    try:
        with _read_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            base_version = drill["version"]
            sealed = bool(drill["sealed"])
            current = _result_from_conn(conn, drill_id)
            records = _load_records(conn, drill_id)
            lock_keys = _load_lock_keys(conn, drill_id)

            merged, changed = _merge_batch_items(records, items)
            try:
                proposed_match = match_records(
                    merged,
                    drill["allowed_diff_secs"],
                    locked_keys=lock_keys,
                )
            except InvalidLockError as exc:
                # 拟写入使既有锁定失效：与正式批量写入同一错误，整批拒绝。
                raise LockInvalidatedError(str(exc))
            proposed = _proposed_result(
                drill_id, sealed, base_version, proposed_match
            )
            if not changed:
                # 同内容重传：确认时不推进版本，拟写入结果即当前结果。
                proposed["version"] = base_version
            changes = _rehearsal_changes(current, proposed)
            secret = _get_service_secret(conn)
        token = rehearsal_tokens.make_token(
            secret, drill_id, base_version, items
        )
        return {
            "drill_id": drill_id,
            "base_version": base_version,
            "sealed": sealed,
            "current_result": current,
            "proposed_result": proposed,
            "would_change_version": changed,
            "changes": changes,
            "normalized_logs": _normalized_log_items(items),
            "token": token,
        }
    finally:
        conn.close()


def commit_batch(drill_id: str, items: Iterable[dict], base_version: int,
                 token: str) -> dict:
    """按预演标识确认提交：标识/基准版本不匹配或演练已封存则拒绝。

    通过校验后按现有批量写入语义在单事务内原子落库；同内容重传不推进
    版本，响应与 ``POST /drills/{id}/logs`` 完全一致（接口兼容）。
    """
    items = list(items)
    _check_batch_unique(items)

    conn = connect()
    try:
        with _write_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            secret = _get_service_secret(conn)
            # 先验证标识本身（按提交方给出的基准版本与规范化批次验签）：
            # 标识不匹配（批次被改、基准号被改、伪造/演练错绑）直接拒绝。
            if not rehearsal_tokens.verify_token(
                secret, token, drill_id, base_version, items
            ):
                raise RehearsalTokenMismatchError(
                    "校验标识与演练、基准版本或规范化批次不匹配"
                )
            # 标识真实有效，但预演后版本已被他人推进 -> 乐观并发冲突。
            if drill["version"] != base_version:
                raise RehearsalVersionConflictError(
                    f"基准版本已变化：预演基准为 {base_version}，"
                    f"当前版本为 {drill['version']}"
                )
            changed = _apply_batch_items(conn, drill_id, items)
            if changed:
                _recompute(conn, drill_id)
        return get_result(drill_id)
    finally:
        conn.close()


def seal_drill(drill_id: str) -> dict:
    conn = connect()
    try:
        with _write_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            conn.execute(
                "UPDATE drills SET sealed = 1, sealed_at = ? WHERE drill_id = ?",
                (_now_iso(), drill_id),
            )
        return get_drill(drill_id)
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 锁定（复核员人工互证）
# --------------------------------------------------------------------------
def _normalize_pair(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a <= b else (b, a)


def _lock_items(
    conn: sqlite3.Connection,
    drill_id: str,
    record_map: dict[str, Record] | None = None,
) -> list[dict]:
    """组装锁定清单（含实时计算的时差），按记录对升序。"""
    rows = conn.execute(
        "SELECT record_a, record_b, created_at FROM locks "
        "WHERE drill_id = ? ORDER BY record_a, record_b",
        (drill_id,),
    ).fetchall()
    if record_map is None:
        record_map = _record_map(_load_records(conn, drill_id))
    items = []
    for row in rows:
        a, b = row["record_a"], row["record_b"]
        diff = None
        if a in record_map and b in record_map:
            diff = abs(
                record_map[a].ts_micros - record_map[b].ts_micros
            ) / MICROS_PER_SECOND
        items.append(
            {
                "record_a": a,
                "record_b": b,
                "time_diff_seconds": diff,
                "locked_at": row["created_at"],
            }
        )
    return items


def _result_from_conn(conn: sqlite3.Connection, drill_id: str) -> dict:
    """在指定连接（可能是未提交的写事务内）读取当前互证结果。"""
    drill = _get_drill_row(conn, drill_id)
    row = conn.execute(
        "SELECT * FROM results WHERE drill_id = ?", (drill_id,)
    ).fetchone()
    if row is None:  # 理论上不会发生（建演练即写空结果）
        return {
            "drill_id": drill_id,
            "version": drill["version"],
            "sealed": bool(drill["sealed"]),
            "pairs": [],
            "unpaired": [],
            "total_time_diff_seconds": 0.0,
        }
    return {
        "drill_id": drill_id,
        "version": row["version"],
        "sealed": bool(drill["sealed"]),
        "pairs": json.loads(row["pairs_json"]),
        "unpaired": json.loads(row["unpaired_json"]),
        "total_time_diff_seconds": row["total_time_diff_seconds"],
        "recomputed_at": row["recomputed_at"],
    }


def _lock_action_response(
    conn: sqlite3.Connection,
    drill_id: str,
    record_map: dict[str, Record] | None = None,
) -> dict:
    locks = _lock_items(conn, drill_id, record_map)
    payload = _result_from_conn(conn, drill_id)
    return {
        "lock": locks[-1] if locks else None,
        "locks": locks,
        "result": payload,
    }


def lock_pair(drill_id: str, record_a: str, record_b: str) -> dict:
    """锁定一对互证候选；重复锁定同一对视为重传（幂等、不推进版本）。"""
    a, b = _normalize_pair(record_a, record_b)
    conn = connect()
    try:
        with _write_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            if a == b:
                raise LockSameRecordError(f"两条记录号不能相同: {a}")

            existing = conn.execute(
                "SELECT 1 FROM locks WHERE drill_id = ? AND record_a = ? "
                "AND record_b = ?",
                (drill_id, a, b),
            ).fetchone()
            if existing is not None:
                # 重复锁定同一对：幂等重传，状态不变、版本不推进。
                return _lock_action_response(conn, drill_id)

            records = _load_records(conn, drill_id)
            rmap = _record_map(records)
            allowed_micros = int(
                round(drill["allowed_diff_secs"] * MICROS_PER_SECOND)
            )
            _check_lock_candidate(rmap, allowed_micros, a, b)

            # 两条记录均不得已被其他锁定占用。
            occupied = conn.execute(
                "SELECT record_a, record_b FROM locks WHERE drill_id = ? "
                "AND (record_a = ? OR record_b = ? OR record_a = ? OR record_b = ?)",
                (drill_id, a, a, b, b),
            ).fetchall()
            if occupied:
                other = occupied[0]
                raise LockOccupiedError(
                    f"记录已被锁定 ({other['record_a']}, {other['record_b']}) "
                    f"占用，不能重复锁定"
                )

            conn.execute(
                "INSERT INTO locks (drill_id, record_a, record_b, created_at) "
                "VALUES (?, ?, ?, ?)",
                (drill_id, a, b, _now_iso()),
            )
            # 锁定状态变化与结果、版本原子提交。
            _recompute(conn, drill_id)
            return _lock_action_response(conn, drill_id, rmap)
    finally:
        conn.close()


def unlock_pair(drill_id: str, record_a: str, record_b: str) -> dict:
    """解除已有锁定；锁定不存在时明确报错（404 lock_not_found）。"""
    a, b = _normalize_pair(record_a, record_b)
    conn = connect()
    try:
        with _write_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            cur = conn.execute(
                "DELETE FROM locks WHERE drill_id = ? AND record_a = ? "
                "AND record_b = ?",
                (drill_id, a, b),
            )
            if cur.rowcount == 0:
                raise LockNotFoundError(f"锁定不存在: ({a}, {b})")
            # 解除锁定后重算：锁定状态变化与结果、版本原子提交。
            _recompute(conn, drill_id)
            return _lock_action_response(conn, drill_id)
    finally:
        conn.close()


def list_locks(drill_id: str) -> dict:
    """查询锁定清单及最新互证结果。"""
    conn = connect()
    try:
        _get_drill_row(conn, drill_id)  # 未知演练 -> 404
        return {
            "drill_id": drill_id,
            "locks": _lock_items(conn, drill_id),
            "result": get_result(drill_id),
        }
    finally:
        conn.close()


def get_drill(drill_id: str) -> dict:
    conn = connect()
    try:
        row = _get_drill_row(conn, drill_id)
        return {
            "drill_id": row["drill_id"],
            "allowed_time_diff_seconds": row["allowed_diff_secs"],
            "version": row["version"],
            "sealed": bool(row["sealed"]),
            "created_at": row["created_at"],
            "sealed_at": row["sealed_at"],
        }
    finally:
        conn.close()


def list_drills() -> list[dict]:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT drill_id FROM drills ORDER BY drill_id"
        ).fetchall()
        return [get_drill(r["drill_id"]) for r in rows]
    finally:
        conn.close()


def list_logs(drill_id: str) -> list[dict]:
    conn = connect()
    try:
        _get_drill_row(conn, drill_id)  # 未知演练 -> 404
        rows = conn.execute(
            "SELECT * FROM logs WHERE drill_id = ? ORDER BY rowid",
            (drill_id,),
        ).fetchall()
        return [
            {
                "record_no": r["record_no"],
                "station": r["station"],
                "counterpart": r["counterpart"],
                "time": r["ts_iso"],
                "band": r["band"],
                "mode": r["mode"],
                "updated_at": r["updated_at"],
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_result(drill_id: str) -> dict:
    """查询当前（封存后即为最终）互证结果。"""
    conn = connect()
    try:
        drill = _get_drill_row(conn, drill_id)
        row = conn.execute(
            "SELECT * FROM results WHERE drill_id = ?", (drill_id,)
        ).fetchone()
        if row is None:  # 理论上不会发生（建演练即写空结果）
            return {
                "drill_id": drill_id,
                "version": drill["version"],
                "sealed": bool(drill["sealed"]),
                "pairs": [],
                "unpaired": [],
                "total_time_diff_seconds": 0.0,
            }
        return {
            "drill_id": drill_id,
            "version": row["version"],
            "sealed": bool(drill["sealed"]),
            "pairs": json.loads(row["pairs_json"]),
            "unpaired": json.loads(row["unpaired_json"]),
            "total_time_diff_seconds": row["total_time_diff_seconds"],
            "recomputed_at": row["recomputed_at"],
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 版本快照（复核员按版本回看 / 比较）：只读，不改变当前状态
# --------------------------------------------------------------------------
def _snapshot_payload(row: sqlite3.Row, sealed: bool) -> dict:
    return {
        "drill_id": row["drill_id"],
        "version": row["version"],
        "sealed": sealed,
        "pairs": json.loads(row["pairs_json"]),
        "unpaired": json.loads(row["unpaired_json"]),
        "total_time_diff_seconds": row["total_time_diff_seconds"],
        "locks": json.loads(row["locks_json"]),
        "snapshot_at": row["created_at"],
    }


def _get_snapshot_row(
    conn: sqlite3.Connection, drill_id: str, version: int
) -> sqlite3.Row:
    """取已留存快照；未知演练抛 drill_not_found，未留存版本抛
    snapshot_not_found。"""
    _get_drill_row(conn, drill_id)
    row = conn.execute(
        "SELECT * FROM snapshots WHERE drill_id = ? AND version = ?",
        (drill_id, version),
    ).fetchone()
    if row is None:
        raise SnapshotNotFoundError(
            f"演练 {drill_id} 的版本 {version} 未留存快照"
            "（升级前的旧版本不可用现态补造）"
        )
    return row


def get_snapshot(drill_id: str, version: int) -> dict:
    """取回指定演练、指定版本已留存的快照（只读，不改变当前状态）。"""
    conn = connect()
    try:
        drill = _get_drill_row(conn, drill_id)
        row = _get_snapshot_row(conn, drill_id, version)
        return _snapshot_payload(row, bool(drill["sealed"]))
    finally:
        conn.close()


def list_snapshots(drill_id: str) -> dict:
    """列出演练已留存快照的版本号（升序）。"""
    conn = connect()
    try:
        _get_drill_row(conn, drill_id)  # 未知演练 -> 404
        rows = conn.execute(
            "SELECT version FROM snapshots WHERE drill_id = ? ORDER BY version",
            (drill_id,),
        ).fetchall()
        return {
            "drill_id": drill_id,
            "versions": [r["version"] for r in rows],
        }
    finally:
        conn.close()


def _diff_pairs_section(pairs_from: list[dict], pairs_to: list[dict]) -> dict:
    """配对按身份比较：added 取新版、removed 取旧版，同对时差变化单列。"""
    pairs_old = sorted(
        pairs_from, key=lambda p: (p["record_a"], p["record_b"])
    )
    pairs_new = sorted(
        pairs_to, key=lambda p: (p["record_a"], p["record_b"])
    )
    old_map = {(p["record_a"], p["record_b"]): p for p in pairs_old}
    new_map = {(p["record_a"], p["record_b"]): p for p in pairs_new}

    added_keys = sorted(set(new_map) - set(old_map))
    removed_keys = sorted(set(old_map) - set(new_map))
    pair_time_diff_changed = [
        {
            "record_a": key[0],
            "record_b": key[1],
            "from_time_diff_seconds": old_map[key]["time_diff_seconds"],
            "to_time_diff_seconds": new_map[key]["time_diff_seconds"],
        }
        for key in sorted(set(old_map) & set(new_map))
        if old_map[key]["time_diff_seconds"]
        != new_map[key]["time_diff_seconds"]
    ]
    return {
        "added": [new_map[k] for k in added_keys],
        "removed": [old_map[k] for k in removed_keys],
        "time_diff_changed": pair_time_diff_changed,
    }


def _diff_unpaired_section(unpaired_from: list[str], unpaired_to: list[str]):
    """未配对记录按记录号集合比较，升序列出新增/移除。"""
    return {
        "added": sorted(set(unpaired_to) - set(unpaired_from)),
        "removed": sorted(set(unpaired_from) - set(unpaired_to)),
    }


def _diff_locks_section(locks_from: list[dict], locks_to: list[dict]) -> dict:
    """锁定按记录对身份比较（仅时差变化不出现，其时差已在配对节体现）。"""
    locks_old = sorted(
        locks_from, key=lambda l: (l["record_a"], l["record_b"])
    )
    locks_new = sorted(
        locks_to, key=lambda l: (l["record_a"], l["record_b"])
    )
    old_lock = {(l["record_a"], l["record_b"]): l for l in locks_old}
    new_lock = {(l["record_a"], l["record_b"]): l for l in locks_new}
    return {
        "added": [new_lock[k] for k in sorted(set(new_lock) - set(old_lock))],
        "removed": [old_lock[k] for k in sorted(set(old_lock) - set(new_lock))],
    }


def _diff_total_section(total_from: float, total_to: float) -> dict:
    return {"from": total_from, "to": total_to, "delta": total_to - total_from}


def _rehearsal_changes(current: dict, proposed: dict) -> dict:
    """预演差异：配对增删、同对时差变化、未配对增删、总时差变化。"""
    return {
        "pairs": _diff_pairs_section(current["pairs"], proposed["pairs"]),
        "unpaired": _diff_unpaired_section(
            current["unpaired"], proposed["unpaired"]
        ),
        "total_time_diff_seconds": _diff_total_section(
            current["total_time_diff_seconds"],
            proposed["total_time_diff_seconds"],
        ),
    }


def _diff_snapshots(
    from_snap: dict, to_snap: dict
) -> dict:
    """比较两个已留存快照：按记录号稳定排序列出配对、未配对、锁定的
    新增/移除；同一记录对的时差变化单列。

    ``added`` 以 to（新）版本内容为准，``removed`` 以 from（旧）版本为准。
    """
    return {
        "drill_id": to_snap["drill_id"],
        "from_version": from_snap["version"],
        "to_version": to_snap["version"],
        "pairs": _diff_pairs_section(from_snap["pairs"], to_snap["pairs"]),
        "unpaired": _diff_unpaired_section(
            from_snap["unpaired"], to_snap["unpaired"]
        ),
        "locks": _diff_locks_section(from_snap["locks"], to_snap["locks"]),
        "total_time_diff_seconds": _diff_total_section(
            from_snap["total_time_diff_seconds"],
            to_snap["total_time_diff_seconds"],
        ),
    }


def compare_snapshots(
    drill_id: str, from_version: int, to_version: int
) -> dict:
    """比较两个已留存版本（只读）。

    相同版本所有差异为空；两个版本都必须已留存，否则明确报错。
    """
    conn = connect()
    try:
        drill = _get_drill_row(conn, drill_id)
        sealed = bool(drill["sealed"])
        if from_version == to_version:
            # 版本也必须已留存：未知/未留存明确报错（不能用现态补造）。
            row = _get_snapshot_row(conn, drill_id, from_version)
            snap = _snapshot_payload(row, sealed)
            return _diff_snapshots(snap, snap)
        from_row = _get_snapshot_row(conn, drill_id, from_version)
        to_row = _get_snapshot_row(conn, drill_id, to_version)
        return _diff_snapshots(
            _snapshot_payload(from_row, sealed),
            _snapshot_payload(to_row, sealed),
        )
    finally:
        conn.close()
