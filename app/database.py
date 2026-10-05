"""SQLite 持久化层。

并发写入由 SQLite 的写事务串行化保证：每次写入都开启
``BEGIN IMMEDIATE`` 立即获取 RESERVED 锁，第二个并发写入会在获取锁时
阻塞/报忙；配合 busy_timeout 重试，所有写入被串行提交。版本号、
日志与配对结果在同一个事务中更新，保证原子性。

启用 WAL 模式以支持“封存/计算期间仍可并发查询最终结果”。
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable

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

-- 服务自身的键值元数据：目前存放批量预演校验标识的 HMAC 密钥
-- （预演本身不落任何状态，密钥随库持久化以保证重启后演练仍可确认）。
CREATE TABLE IF NOT EXISTS meta (
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


class PreviewTokenMismatchError(ServiceError):
    """确认时提交的校验标识与“演练+基准版本+规范化批次”不符，或演练已封存。"""

    status_code = 409
    error_code = "preview_token_mismatch"


class PreviewBaseVersionChangedError(ServiceError):
    """确认时演练当前版本已不同于预演基准版本。"""

    status_code = 409
    error_code = "preview_base_version_changed"


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
        # 批量预演校验标识所需的 HMAC 密钥（不存在即生成，随库持久化）。
        _ensure_preview_secret(conn)
        # 升级既有库：从升级时的当前版本开始留存快照；更早的版本不补造。
        _backfill_legacy_snapshots(conn)
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 批量预演校验标识：HMAC-SHA256(drill + 基准版本 + 规范化批次)，无状态
# --------------------------------------------------------------------------
PREVIEW_SECRET_KEY = "preview_hmac_secret"
_PREVIEW_TOKEN_PREFIX = "pv1"

# 进程内缓存密钥：预演/确认只读，密钥随库持久化，缺失时补生成。
_preview_secret_cache: str | None = None


def _ensure_preview_secret(conn: sqlite3.Connection) -> str:
    """确保库中存在预演 HMAC 密钥并缓存；不存在则生成 32 字节随机串。"""
    global _preview_secret_cache
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (PREVIEW_SECRET_KEY,)
    ).fetchone()
    if row is not None and row["value"]:
        _preview_secret_cache = row["value"]
        return _preview_secret_cache
    secret = secrets.token_hex(32)
    # INSERT OR IGNORE：并发初始化时以已落库者为准。
    conn.execute(
        "INSERT OR IGNORE INTO meta (key, value) VALUES (?, ?)",
        (PREVIEW_SECRET_KEY, secret),
    )
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (PREVIEW_SECRET_KEY,)
    ).fetchone()
    _preview_secret_cache = row["value"]
    return _preview_secret_cache


def _server_secret() -> str:
    """取预演 HMAC 密钥（缓存优先；缓存为空时落库取/建一个）。"""
    global _preview_secret_cache
    if _preview_secret_cache:
        return _preview_secret_cache
    conn = connect()
    try:
        return _ensure_preview_secret(conn)
    finally:
        conn.close()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _preview_signing_payload(
    drill_id: str, base_version: int, items: list[dict]
) -> bytes:
    """构造“演练 + 基准版本 + 规范化批次”的确定性字节串。"""
    canonical = {
        "drill_id": drill_id,
        "base_version": base_version,
        "logs": [
            {
                "record_no": it["record_no"],
                "station": it["station"],
                "counterpart": it["counterpart"],
                "ts_micros": it["ts_micros"],
                "band": it["band"],
                "mode": it["mode"],
            }
            for it in items
        ],
    }
    return json.dumps(
        canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _make_preview_token(
    drill_id: str, base_version: int, items: list[dict]
) -> str:
    payload = _preview_signing_payload(drill_id, base_version, items)
    mac = hmac.new(
        _server_secret().encode("ascii"), payload, hashlib.sha256
    ).digest()
    return f"{_PREVIEW_TOKEN_PREFIX}.{_b64url(payload)}.{_b64url(mac)}"


def _verify_preview_token(
    token: str, drill_id: str, base_version: int, items: list[dict]
) -> bool:
    """恒定时间比对校验标识是否绑定该演练/基准版本/规范化批次。"""
    try:
        prefix, payload_b64, mac_b64 = token.split(".", 2)
        if prefix != _PREVIEW_TOKEN_PREFIX:
            return False
        payload = _b64url_decode(payload_b64)
        mac = _b64url_decode(mac_b64)
    except (ValueError, binascii.Error):
        return False
    expected = _preview_signing_payload(drill_id, base_version, items)
    expected_mac = hmac.new(
        _server_secret().encode("ascii"), expected, hashlib.sha256
    ).digest()
    return hmac.compare_digest(payload, expected) and hmac.compare_digest(
        mac, expected_mac
    )


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


@contextmanager
def _read_tx(conn: sqlite3.Connection):
    """延迟读事务：WAL 下整段读取同一一致性快照，不获取写锁、不写库。"""
    conn.execute("BEGIN")
    try:
        yield
    finally:
        conn.execute("ROLLBACK")


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


def _check_batch_unique(items: list[dict]) -> None:
    """批次内同一记录号出现多次视为非法（无法判定终态）。"""
    seen: set[str] = set()
    for it in items:
        if it["record_no"] in seen:
            raise DuplicateRecordInBatchError(
                f"批次内记录号重复: {it['record_no']}"
            )
        seen.add(it["record_no"])


def _apply_batch_items(
    conn: sqlite3.Connection, drill_id: str, items: list[dict]
) -> bool:
    """在写事务内按批量写入语义 upsert 整批日志，返回是否有任一记录变化。

    同内容（规范化后一致）不写；新记录插入、异内容更正。不负责重算，
    由调用方在有变化时触发全场重算（重算内含锁定有效性校验，
    失效抛 ``lock_invalidated``，事务随异常整批回滚）。
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
# 批量写入预演与确认（复核员提交前预演）
#
# 预演是只读操作：在 WAL 一致性读快照上把整批 upsert 在内存中模拟一遍，
# 沿用与正式写入完全相同的规范化、批内重复校验、锁定有效性校验与全场
# 最优配对规则，输出当前结果与拟写入结果；不改日志、锁定、结果、版本、
# 快照。返回的校验标识（无状态 HMAC）绑定“演练 + 基准版本 + 规范化批次”。
# 确认在写事务内核验封存/基准版本/标识后，完全走现有批量写入语义。
# --------------------------------------------------------------------------
def _result_payload_from_match(
    drill_id: str,
    version: int,
    sealed: bool,
    result: MatchResult,
    recomputed_at: str | None = None,
) -> dict:
    return {
        "drill_id": drill_id,
        "version": version,
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
        "recomputed_at": recomputed_at,
    }


def _simulate_batch(
    records: list[Record], items: list[dict]
) -> list[Record]:
    """在内存中按批量 upsert 语义模拟整批写入（新记录追加、异内容替换）。"""
    by_no = {r.record_no: i for i, r in enumerate(records)}
    simulated = list(records)
    for it in items:
        candidate = Record(
            record_no=it["record_no"],
            station=it["station"],
            counterpart=it["counterpart"],
            ts_micros=it["ts_micros"],
            band=it["band"],
            mode=it["mode"],
        )
        idx = by_no.get(it["record_no"])
        if idx is None:
            by_no[it["record_no"]] = len(simulated)
            simulated.append(candidate)
        else:
            simulated[idx] = candidate
    return simulated


def _batch_changes_records(
    record_map: dict[str, Record], items: list[dict]
) -> bool:
    """按批量 upsert 语义判断整批是否含任一“新增或异内容更正”记录。"""
    for it in items:
        existing = record_map.get(it["record_no"])
        if existing is None:
            return True
        if (
            existing.station, existing.counterpart, existing.ts_micros,
            existing.band, existing.mode,
        ) != (
            it["station"], it["counterpart"], it["ts_micros"],
            it["band"], it["mode"],
        ):
            return True
    return False


def preview_batch_logs(drill_id: str, items: Iterable[dict]) -> dict:
    """只读预演：返回当前版本、拟写入完整结果、配对/未配对差异与校验标识。

    非法记录（由请求模型拦截）、批内记录号重复、演练已封存、批次使任一
    既有锁定失效均按现有批量写入的规则报错；预演不改变任何状态。
    """
    items = list(items)
    _check_batch_unique(items)

    conn = connect()
    try:
        with _read_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            base_version = drill["version"]
            records = _load_records(conn, drill_id)
            lock_keys = _load_lock_keys(conn, drill_id)

            simulated = _simulate_batch(records, items)
            try:
                proposed = match_records(
                    simulated, drill["allowed_diff_secs"],
                    locked_keys=lock_keys,
                )
            except InvalidLockError as exc:
                raise LockInvalidatedError(str(exc))

            current = _result_from_conn(conn, drill_id)
            changed = _batch_changes_records(_record_map(records), items)
        proposed_version = base_version + 1 if changed else base_version
        proposed_payload = _result_payload_from_match(
            drill_id, proposed_version, False, proposed
        )
        token = _make_preview_token(drill_id, base_version, items)
        return {
            "drill_id": drill_id,
            "base_version": base_version,
            "current_result": current,
            "proposed_version": proposed_version,
            "would_advance_version": changed,
            "proposed_result": proposed_payload,
            "diff": {
                "pairs": _pairs_diff(current["pairs"], proposed_payload["pairs"]),
                "unpaired": _unpaired_diff(
                    current["unpaired"], proposed_payload["unpaired"]
                ),
                "total_time_diff_seconds": {
                    "from": current["total_time_diff_seconds"],
                    "to": proposed_payload["total_time_diff_seconds"],
                    "delta": (
                        proposed_payload["total_time_diff_seconds"]
                        - current["total_time_diff_seconds"]
                    ),
                },
            },
            "normalized_batch": [
                {
                    "record_no": it["record_no"],
                    "station": it["station"],
                    "counterpart": it["counterpart"],
                    "time": _micros_to_iso(it["ts_micros"]),
                    "band": it["band"],
                    "mode": it["mode"],
                }
                for it in items
            ],
            "token": token,
        }
    finally:
        conn.close()


def confirm_batch_logs(
    drill_id: str, items: Iterable[dict], base_version: int, token: str
) -> dict:
    """按预演校验标识确认提交：标识不符/版本已变化/已封存均拒绝且不改状态。

    校验通过后完全沿用批量写入语义（整批 upsert、有变化才重算推进版本、
    同内容重传不推进版本）在一个原子事务内落库。
    """
    items = list(items)
    _check_batch_unique(items)

    conn = connect()
    try:
        with _write_tx(conn):
            drill = _get_drill_row(conn, drill_id)
            _require_unsealed(drill)
            if drill["version"] != base_version:
                raise PreviewBaseVersionChangedError(
                    f"演练 {drill_id} 当前版本 {drill['version']} "
                    f"与预演基准版本 {base_version} 不一致，请重新预演"
                )
            if not _verify_preview_token(
                token, drill_id, base_version, items
            ):
                raise PreviewTokenMismatchError(
                    "校验标识与演练、基准版本或规范化批次不匹配，"
                    "可能批次已改动或演练已封存，请重新预演"
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


def _pairs_diff(pairs_old: list[dict], pairs_new: list[dict]) -> dict:
    """按记录对身份比较两份配对清单：新增/移除/同对时差变化。

    ``added`` 以新清单内容为准，``removed`` 以旧清单内容为准。
    """
    pairs_old = sorted(pairs_old, key=lambda p: (p["record_a"], p["record_b"]))
    pairs_new = sorted(pairs_new, key=lambda p: (p["record_a"], p["record_b"]))
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


def _unpaired_diff(unpaired_old: list[str], unpaired_new: list[str]) -> dict:
    """未配对记录号集合差异（按记录号升序）。"""
    return {
        "added": sorted(set(unpaired_new) - set(unpaired_old)),
        "removed": sorted(set(unpaired_old) - set(unpaired_new)),
    }


def _diff_snapshots(
    from_snap: dict, to_snap: dict
) -> dict:
    """比较两个已留存快照：按记录号稳定排序列出配对、未配对、锁定的
    新增/移除；同一记录对的时差变化单列。

    ``added`` 以 to（新）版本内容为准，``removed`` 以 from（旧）版本为准。
    """
    locks_old = sorted(
        from_snap["locks"], key=lambda l: (l["record_a"], l["record_b"])
    )
    locks_new = sorted(
        to_snap["locks"], key=lambda l: (l["record_a"], l["record_b"])
    )
    old_lock = {(l["record_a"], l["record_b"]): l for l in locks_old}
    new_lock = {(l["record_a"], l["record_b"]): l for l in locks_new}

    return {
        "drill_id": to_snap["drill_id"],
        "from_version": from_snap["version"],
        "to_version": to_snap["version"],
        "pairs": _pairs_diff(from_snap["pairs"], to_snap["pairs"]),
        "unpaired": _unpaired_diff(
            from_snap["unpaired"], to_snap["unpaired"]
        ),
        "locks": {
            "added": [new_lock[k] for k in sorted(set(new_lock) - set(old_lock))],
            "removed": [old_lock[k] for k in sorted(set(old_lock) - set(new_lock))],
        },
        "total_time_diff_seconds": {
            "from": from_snap["total_time_diff_seconds"],
            "to": to_snap["total_time_diff_seconds"],
            "delta": (
                to_snap["total_time_diff_seconds"]
                - from_snap["total_time_diff_seconds"]
            ),
        },
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
