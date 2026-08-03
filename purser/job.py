"""A unit of work on the queue.

Jobs are stored as JSON. The Lua scripts that run inside Redis read and write
the same fields (``id``, ``attempts``, ``tier``), so the shapes must stay in
sync — keep this dataclass and the scripts in :mod:`purser.queue` aligned.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field

TIERS = ("hot", "warm", "cold")


@dataclass
class Job:
    payload: str
    tier: str = "warm"
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    attempts: int = 0
    enqueued_at: float = field(default_factory=time.time)
    meta: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "id": self.id,
                "payload": self.payload,
                "tier": self.tier,
                "attempts": self.attempts,
                "enqueued_at": self.enqueued_at,
                "meta": self.meta or {},
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, raw: str) -> "Job":
        d = json.loads(raw)
        meta = d.get("meta")
        if not isinstance(meta, dict):  # cjson may round-trip {} oddly
            meta = {}
        return cls(
            payload=d["payload"],
            tier=d.get("tier", "warm"),
            id=d.get("id", uuid.uuid4().hex),
            attempts=int(d.get("attempts", 0)),
            enqueued_at=float(d.get("enqueued_at", time.time())),
            meta=meta,
        )
