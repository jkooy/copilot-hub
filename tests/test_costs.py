from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from copilot_hub.costs import (
    NoLedger,
    Request,
    SpendMonitor,
    estimate_cost,
    load_ledger,
)


def request(key, when, *, session="s", cost=None, input_tokens=1000, billed=None):
    timestamp = datetime.fromisoformat(when)
    priced = estimate_cost(
        "claude-opus-5-5",
        input_tokens=input_tokens,
        output_tokens=0,
        cache_read_tokens=0,
        cache_write_tokens=0,
    )
    return Request(
        key=key,
        timestamp=timestamp,
        session_id=session,
        model="claude-opus-5-5",
        input_tokens=input_tokens,
        output_tokens=0,
        cache_read_tokens=0,
        cache_write_tokens=0,
        cost_usd=priced if cost is None else cost,
        billed=cost is not None if billed is None else billed,
    )


class FakeLedger:
    label = "Example gateway billing"

    def __init__(self, requests):
        self.requests = requests

    def available(self):
        return True

    def requests_between(self, start, end):
        return [item for item in self.requests if start <= item.timestamp < end]


def test_list_prices():
    assert estimate_cost(
        "claude-sonnet-5-5",
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        cache_read_tokens=1_000_000,
        cache_write_tokens=1_000_000,
    ) == pytest.approx(2 + 10 + 0.2 + 2.5)
    assert estimate_cost(
        "claude-opus-5-5",
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        cache_read_tokens=1_000_000,
        cache_write_tokens=1_000_000,
    ) == pytest.approx(4 + 20 + 0.2 + 5)
    # Unknown models are priced as the most expensive known model.
    assert estimate_cost(
        "mystery",
        input_tokens=1_000_000,
        output_tokens=0,
        cache_read_tokens=0,
        cache_write_tokens=0,
    ) == pytest.approx(4)


def test_ledger_spend_is_attributed_to_hub_sessions(tmp_path):
    ledger = FakeLedger(
        [
            request("a", "2026-10-02T18:00:00+00:00", session="mgr", cost=1.5),
            request("b", "2026-10-02T18:05:00+00:00", session="w1", input_tokens=1_000_000),
            request("c", "2026-10-02T18:06:00+00:00", session="elsewhere", cost=2.0),
            request("d", "2026-10-01T18:00:00+00:00", session="mgr", cost=50),
        ]
    )
    monitor = SpendMonitor(daily_budget_usd=10, timezone="America/Los_Angeles", ledger=ledger)
    now = datetime(2026, 10, 2, 20, 0, tzinfo=UTC)

    snapshot = monitor.snapshot({"manager": {"mgr"}, "worker": {"w1", "w1-old"}}, now=now)

    assert snapshot["source"] == "ledger"
    assert snapshot["source_label"] == "Example gateway billing"
    assert snapshot["spent_usd"] == pytest.approx(1.5 + 4.0 + 2.0)
    assert snapshot["remaining_usd"] == pytest.approx(2.5)
    assert snapshot["status"] == "normal"
    assert snapshot["hub"]["cost_usd"] == pytest.approx(5.5)
    assert snapshot["workers"]["manager"]["cost_usd"] == pytest.approx(1.5)
    assert snapshot["workers"]["worker"]["cost_usd"] == pytest.approx(4.0)
    assert snapshot["workers"]["worker"]["estimated_requests"] == 1
    assert snapshot["total"]["requests"] == 3

    ledger.requests.append(request("e", "2026-10-02T19:00:00+00:00", session="w1", cost=1.0))
    assert monitor.snapshot({"worker": {"w1"}}, now=now)["status"] == "warning"


def test_day_boundary_follows_the_budget_time_zone():
    ledger = FakeLedger([request("late", "2026-10-03T01:00:00+00:00", cost=3)])
    now = datetime(2026, 10, 3, 2, 0, tzinfo=UTC)

    pacific = SpendMonitor(daily_budget_usd=3, timezone="America/Los_Angeles", ledger=ledger)
    utc = SpendMonitor(daily_budget_usd=3, timezone="UTC", ledger=ledger)

    # 01:00 UTC on Oct 3 is still Oct 2 in Los Angeles.
    assert pacific.snapshot({}, now=now)["spent_usd"] == 3
    assert pacific.snapshot({}, now=now)["status"] == "exhausted"
    assert utc.snapshot({}, now=datetime(2026, 10, 3, 12, tzinfo=UTC))["spent_usd"] == 3
    assert utc.snapshot({}, now=datetime(2026, 10, 2, 12, tzinfo=UTC))["spent_usd"] == 0


def test_without_a_ledger_spend_comes_from_hub_transcripts(tmp_path):
    transcript = tmp_path / "t.jsonl"
    usage = {"input_tokens": 1_000_000, "output_tokens": 0}
    transcript.write_text(
        "\n".join(
            json.dumps(entry)
            for entry in [
                {"type": "assistant", "timestamp": "2026-10-02T10:00:00Z",
                 "message": {"id": "m1", "model": "claude-opus-5-5", "usage": usage}},
                {"type": "assistant", "timestamp": "2026-10-02T10:00:01Z",
                 "message": {"id": "m1", "model": "claude-opus-5-5", "usage": usage}},
                {"type": "assistant", "timestamp": "2026-10-01T10:00:00Z",
                 "message": {"id": "m0", "model": "claude-opus-5-5", "usage": usage}},
            ]
        ),
        encoding="utf-8",
    )
    monitor = SpendMonitor(daily_budget_usd=300, timezone="UTC")

    snapshot = monitor.snapshot(
        {"worker": {"s"}},
        {"worker": [transcript]},
        now=datetime(2026, 10, 2, 20, tzinfo=UTC),
    )

    assert snapshot["source"] == "transcripts"
    assert snapshot["source_label"] == "Estimated from Hub transcripts"
    assert snapshot["spent_usd"] == 4.0
    assert snapshot["workers"]["worker"]["requests"] == 1


def test_ledger_plugins_load_from_a_file_or_module(tmp_path):
    plugin = tmp_path / "my_ledger.py"
    plugin.write_text(
        "from dataclasses import dataclass\n"
        "@dataclass\n"
        "class Cursor:\n"
        "    last_id: int = 0\n"
        "class Ledger:\n"
        "    label = 'Local ledger'\n"
        "    def available(self):\n"
        "        return True\n"
        "    def requests_between(self, start, end):\n"
        "        return []\n",
        encoding="utf-8",
    )

    assert load_ledger(f"{plugin}:Ledger").label == "Local ledger"
    assert isinstance(load_ledger("copilot_hub.costs:NoLedger"), NoLedger)
    assert isinstance(load_ledger(None), NoLedger)
    # A broken plugin falls back to transcript estimates instead of failing startup.
    assert isinstance(load_ledger(f"{tmp_path / 'missing.py'}:Ledger"), NoLedger)
