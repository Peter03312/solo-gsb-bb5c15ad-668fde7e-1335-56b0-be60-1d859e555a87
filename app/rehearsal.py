"""批量写入预演（rehearsal）的校验标识（token）。

标识为**无状态**签名：不依赖任何持久化的演练记录，因此预演本身不写库、
不留存任何状态（日志、锁定、结果、版本、快照均不变）。标识把三件事
绑定在一起：

1. **演练**（``drill_id``）；
2. **基准版本**（预演时的当前 ``version``）；
3. **规范化批次**（服务端规范化、整批去重后的终态批次，JSON 规范化后
   参与签名），客户端无需重新规范化即可原样回传。

确认提交时重新计算签名并用恒定时间比较：标识不匹配、基准版本已变化
（他人已推进版本/已封存演练）、或标识已随服务密钥轮换而失效（演练
封存）均拒绝且不改变状态。

签名密钥（HMAC-SHA256）在服务启动初始化时生成并留存于服务元数据表，
升级既有库时幂等补生成；密钥不随预演落库，只用于本服务内验签。
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Sequence

TOKEN_PREFIX = "rehearse1"
# 域分隔前缀：防止同一密钥的签名被挪作其他用途。
_MESSAGE_PREFIX = b"qso-batch-rehearse/v1\n"


def canonical_batch_json(items: Sequence[dict]) -> bytes:
    """规范化批次的确定性 JSON（排序键、无空白、ensure_ascii=False）。

    预演与确认两端以同一序列化结果参与签名，保证“规范化批次”绑定一致。
    """

    return json.dumps(
        list(items), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _signing_message(
    drill_id: str, base_version: int, batch_json: bytes
) -> bytes:
    return _MESSAGE_PREFIX + b"\x1f".join(
        [
            drill_id.encode("utf-8"),
            str(int(base_version)).encode("ascii"),
            batch_json,
        ]
    )


def make_token(
    secret: str, drill_id: str, base_version: int, items: Sequence[dict]
) -> str:
    """生成绑定演练、基准版本与规范化批次的校验标识。"""

    digest = hmac.new(
        secret.encode("utf-8"),
        _signing_message(drill_id, base_version, canonical_batch_json(items)),
        hashlib.sha256,
    ).hexdigest()
    return f"{TOKEN_PREFIX}.{digest}"


def verify_token(
    secret: str,
    token: str,
    drill_id: str,
    base_version: int,
    items: Sequence[dict],
) -> bool:
    """恒定时间比较地校验标识是否与演练/基准版本/规范化批次匹配。"""

    expected = make_token(secret, drill_id, base_version, items)
    return hmac.compare_digest(expected, token)
