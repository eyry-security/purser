from __future__ import annotations

import uuid

import pytest
import redis

from purser import EnqueueResult, Purser


@pytest.fixture
def queue() -> Purser:
    client = redis.Redis.from_url(
        "redis://127.0.0.1:6379",
        decode_responses=True,
        socket_connect_timeout=0.25,
        socket_timeout=0.25,
    )
    try:
        client.ping()
    except redis.RedisError:
        pytest.skip("Redis is not available on localhost")

    instance = Purser(client=client, prefix=f"purser:test:{uuid.uuid4().hex}")
    instance.purge()
    try:
        yield instance
    finally:
        instance.purge()
        client.close()


def test_enqueue_dequeue_ack_round_trip(queue: Purser) -> None:
    assert queue.enqueue(
        "api.example.test",
        tier="warm",
        meta={"source": "fixture"},
    ) is EnqueueResult.ENQUEUED
    assert queue.stats().as_dict() == {
        "hot": 0,
        "warm": 1,
        "cold": 0,
        "ready": 1,
        "inflight": 0,
        "dlq": 0,
        "seen": 1,
    }

    job = queue.dequeue()

    assert job is not None
    assert job.payload == "api.example.test"
    assert job.tier == "warm"
    assert job.meta == {"source": "fixture"}
    assert queue.stats().ready == 0
    assert queue.stats().inflight == 1

    queue.ack(job)

    assert queue.stats().inflight == 0
    assert queue.dequeue() is None


def test_dequeue_uses_strict_priority_and_fifo_within_lanes(queue: Purser) -> None:
    for payload, tier in (
        ("cold-1", "cold"),
        ("hot-1", "hot"),
        ("warm-1", "warm"),
        ("hot-2", "hot"),
        ("cold-2", "cold"),
    ):
        assert queue.enqueue(payload, tier=tier) is EnqueueResult.ENQUEUED

    claimed = []
    while (job := queue.dequeue()) is not None:
        claimed.append(job.payload)
        queue.ack(job)

    assert claimed == ["hot-1", "hot-2", "warm-1", "cold-1", "cold-2"]
    assert queue.stats().ready == 0
    assert queue.stats().inflight == 0


def test_dedup_and_backpressure_do_not_poison_seen_set(queue: Purser) -> None:
    queue.max_depth = 1

    assert queue.enqueue("first", tier="hot") is EnqueueResult.ENQUEUED
    assert queue.enqueue("first", tier="hot") is EnqueueResult.DUPLICATE
    assert queue.enqueue("second", tier="hot") is EnqueueResult.FULL
    assert queue.stats().seen == 1

    first = queue.dequeue()
    assert first is not None
    queue.ack(first)

    assert queue.enqueue("second", tier="hot") is EnqueueResult.ENQUEUED
    assert queue.stats().seen == 2
