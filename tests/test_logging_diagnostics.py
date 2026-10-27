from __future__ import annotations

import asyncio
import json
import logging
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from betterer_ratings.core.urls import sanitize_url_for_logs
from betterer_ratings.domain.models import HarvestCycleResult, PMDBSubmitResult
from betterer_ratings.infra.http.client import HTTPClient
from betterer_ratings.observability.log_buffer import LogBufferHandler
from betterer_ratings.observability.logging_setup import JsonLogFormatter
from betterer_ratings.services.harvest.cycle_runner import run_harvester
from betterer_ratings.services.harvest.mal import fetch_candidate_mal_score
from betterer_ratings.services.submit.handler_mapping import submit_mapping
from betterer_ratings.services.submit.handler_rating import submit_rating


def record(message, args=(), **extra):
    result = logging.LogRecord(
        "betterer-ratings", logging.WARNING, __file__, 30, message, args, None
    )
    result.__dict__.update(extra)
    return result


@pytest.mark.parametrize(
    "label,component",
    [
        ("PMDB", "pmdb"),
        ("MAL", "mal"),
        ("Jikan", "jikan"),
        ("Fribb", "fribb"),
        ("Dins", "dins"),
        ("AnimeOfflineDatabase", "anime_offline_database"),
    ],
)
def test_provider_context_and_event_are_stable_across_values(label, component):
    formatter = JsonLogFormatter()
    first = json.loads(formatter.format(record(f"[{label}] Refreshed %s rows.", (10,))))
    second = json.loads(formatter.format(record(f"[{label}] Refreshed %s rows.", (999,))))
    assert first["provider"] == first["component"] == component
    assert first["event"] == second["event"] == "refreshed_rows"
    assert first["message"] != second["message"]


def test_stdout_and_dashboard_share_timestamp_and_redacted_exception_context():
    credentials = {"Authorization": "Bearer header-secret", "api_key": "nested-secret"}
    try:
        raise ValueError(
            "GET https://user:pass@example.test/data?api_key=url-secret Bearer token-secret"
        )
    except ValueError:
        entry = record(
            "[PMDB] Failed: password='text-secret'",
            details=[credentials],
            timestamp=0,
            event="mapping.failed",
        )
        entry.exc_info = sys.exc_info()

    formatted = JsonLogFormatter().format(entry)
    payload = json.loads(formatted)
    handler = LogBufferHandler()
    handler.handle(entry)
    assert handler.snapshot() == [payload]
    assert payload["timestamp"] == entry.created
    assert payload["event"] == "mapping.failed"
    assert payload["code_location"].endswith(":30")
    assert "ValueError" in payload["exception"]
    assert payload["details"] == [{"Authorization": "***", "api_key": "***"}]
    for secret in (
        "header-secret",
        "nested-secret",
        "url-secret",
        "token-secret",
        "text-secret",
        "user:pass",
    ):
        assert secret not in formatted
    assert credentials["api_key"] == "nested-secret"


def test_url_credentials_are_masked_even_without_query():
    assert (
        sanitize_url_for_logs("https://user:pass@example.test/data")
        == "https://***@example.test/data"
    )


@pytest.mark.parametrize("status", [429, 503, 0])
def test_http_retry_and_recovery_share_request_id_and_duration(monkeypatch, caplog, status):
    async def no_sleep(_seconds):
        pass

    monkeypatch.setattr("betterer_ratings.infra.http.client.asyncio.sleep", no_sleep)
    outcomes = iter([status, 200])

    def respond(request):
        next_status = next(outcomes)
        if next_status == 0:
            raise httpx.ReadError("connection dropped", request=request)
        return httpx.Response(next_status, json={"ok": next_status == 200})

    async def run():
        client = HTTPClient(
            5, 2, client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(respond))
        )
        try:
            return await client.request_json(method="GET", url="https://example.test/data")
        finally:
            await client.aclose()

    with caplog.at_level(logging.INFO, logger="betterer-ratings"):
        result = asyncio.run(run())
    events = [item for item in caplog.records if hasattr(item, "request_id")]
    assert result.status == 200
    assert len({item.request_id for item in events}) == 1
    summary = events[-1]
    assert summary.event == "http.request.completed"
    assert summary.outcome == "success"
    assert summary.attempts == 2
    assert summary.recovered is True
    assert summary.elapsed_ms >= 0
    assert summary.levelno == logging.INFO


@pytest.mark.parametrize("status", [429, 503, 0])
def test_http_final_attempt_never_promises_another_retry(caplog, status):
    def respond(request):
        if status == 0:
            raise httpx.ConnectError("offline", request=request)
        return httpx.Response(status, json={"error": "unavailable"})

    async def run():
        client = HTTPClient(
            5, 1, client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(respond))
        )
        try:
            return await client.request_json(method="GET", url="https://example.test/data")
        finally:
            await client.aclose()

    with caplog.at_level(logging.INFO, logger="betterer-ratings"):
        result = asyncio.run(run())
    assert result.status == status
    for item in caplog.records:
        if hasattr(item, "retrying"):
            assert item.retrying is False
            assert item.backoff_seconds == 0
    summary = caplog.records[-1]
    assert summary.event == "http.request.completed"
    assert summary.levelno == logging.WARNING
    assert summary.attempts == 1
    assert summary.recovered is False
    if status == 0:
        assert summary.error_type == "ConnectError"


@pytest.mark.parametrize("status", [200, 404])
def test_normal_success_and_expected_missing_lookup_stay_at_debug(caplog, status):
    async def run():
        client = HTTPClient(
            5,
            1,
            client_factory=lambda: httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(status, json={})),
            ),
        )
        try:
            await client.request_json(method="GET", url="https://example.test/data")
            await client.request_json(method="GET", url="https://example.test/data")
        finally:
            await client.aclose()

    with caplog.at_level(logging.DEBUG, logger="betterer-ratings"):
        asyncio.run(run())
    events = [
        item for item in caplog.records if getattr(item, "event", "") == "http.request.completed"
    ]
    assert len(events) == 2
    assert all(item.levelno == logging.DEBUG for item in events)
    assert events[0].request_id != events[1].request_id


def test_paused_gate_is_deferred_without_a_network_attempt(caplog):
    gate = SimpleNamespace(name="tmdb", acquire=AsyncMock(return_value=False))
    factory = Mock(side_effect=AssertionError("No HTTP client should be created"))
    with caplog.at_level(logging.DEBUG, logger="betterer-ratings"):
        response = asyncio.run(
            HTTPClient(5, 2, client_factory=factory).request_json(
                method="GET",
                url="https://example.test/data",
                gate=gate,
            )
        )
    assert response.status == 429
    event = caplog.records[-1]
    assert event.outcome == "deferred"
    assert event.attempts == 0
    assert event.provider == "tmdb"
    assert event.levelno == logging.DEBUG


@pytest.mark.parametrize("kind", ["rating", "mapping"])
@pytest.mark.parametrize(
    "retryable,attempts,expected",
    [
        (True, 0, "retry_scheduled"),
        (True, 2, "failed"),
        (False, 0, "failed"),
    ],
)
def test_submission_failures_include_actionable_context(
    caplog, kind, retryable, attempts, expected
):
    result = PMDBSubmitResult(
        success=False,
        retryable=retryable,
        retry_after_seconds=30,
        duplicate_or_exists=False,
        error_text="provider unavailable",
        item_id=None,
        status_code=503,
        error_code="service_unavailable",
        endpoint=f"/api/external/{kind}s",
    )
    client = SimpleNamespace(
        submit_rating=AsyncMock(return_value=result), submit_mapping=AsyncMock(return_value=result)
    )
    db = Mock()
    db.defer_held_mapping.return_value = False
    row = {
        "tmdb_id": 128,
        "media_type": "movie",
        "pmdb_item_id": None,
        "pmdb_attempts": attempts,
        "id_type": "imdb",
        "id_value": "tt0119698",
        "label": "IM",
        "score": 90.0,
    }
    submit = submit_rating if kind == "rating" else submit_mapping
    with caplog.at_level(logging.INFO, logger="betterer-ratings"):
        asyncio.run(
            submit(
                row=row,
                pmdb_client=client,
                db=db,
                verify_after_transient_statuses=set(),
                max_retry_attempts=3,
                format_manual_error_fn=lambda **kwargs: "failed",
                format_pmdb_error_fn=lambda **kwargs: "retry",
                retry_delay_seconds_fn=lambda *_: 30,
                now_epoch_fn=lambda: 1000,
                first_non_empty_fn=lambda value: value,
                logger=logging.getLogger("betterer-ratings"),
            )
        )
    event = caplog.records[-1]
    assert event.event == f"{kind}.{expected}"
    assert event.tmdb_id == 128
    assert event.media_type == "movie"
    assert event.status == 503
    assert event.error_code == "service_unavailable"
    assert event.attempt == attempts + 1
    if expected == "retry_scheduled":
        assert event.retry_at == 1030
        assert event.backoff_seconds == 30
    else:
        assert event.reason == ("max_attempts" if retryable else "non_retryable")


@pytest.mark.parametrize(
    "selected,errors,interrupted,outcome,level",
    [
        (0, 0, False, "success", logging.DEBUG),
        (10, 0, False, "success", logging.INFO),
        (10, 2, False, "partial", logging.WARNING),
        (10, 0, True, "interrupted", logging.INFO),
    ],
)
def test_harvest_cycle_summary_distinguishes_idle_work_errors_and_interruption(
    caplog,
    selected,
    errors,
    interrupted,
    outcome,
    level,
):
    stop = asyncio.Event()

    async def cycle(_stop):
        stop.set()
        return HarvestCycleResult(selected, errors, 0, interrupted)

    harvester = SimpleNamespace(_run_cycle=cycle)
    with caplog.at_level(logging.DEBUG, logger="betterer-ratings"):
        asyncio.run(
            run_harvester(
                harvester=harvester,
                stop_event=stop,
                logger=logging.getLogger("betterer-ratings"),
                now_epoch_fn=lambda: 1000,
            )
        )
    summary = next(
        item for item in caplog.records if getattr(item, "event", "") == "harvest.cycle.completed"
    )
    assert summary.outcome == outcome
    assert summary.selected_candidates == selected
    assert summary.tmdb_list_request_errors == errors
    assert summary.elapsed_ms >= 0
    assert summary.levelno == level


def test_harvest_failure_preserves_traceback_and_duration(caplog):
    stop = asyncio.Event()

    async def cycle(_stop):
        stop.set()
        raise RuntimeError("broken cycle")

    with caplog.at_level(logging.DEBUG, logger="betterer-ratings"):
        asyncio.run(
            run_harvester(
                harvester=SimpleNamespace(_run_cycle=cycle),
                stop_event=stop,
                logger=logging.getLogger("betterer-ratings"),
                now_epoch_fn=lambda: 1000,
            )
        )
    failure = next(
        item for item in caplog.records if getattr(item, "event", "") == "harvest.cycle.failed"
    )
    assert failure.elapsed_ms >= 0
    assert failure.exc_info is not None
    assert not any(
        getattr(item, "event", "") == "harvest.cycle.completed" for item in caplog.records
    )


@pytest.mark.parametrize("conflict", [False, True])
def test_mal_missing_mapping_is_quiet_but_identity_conflict_is_retained(caplog, conflict):
    db = SimpleNamespace(
        get_title_mapping=lambda **kwargs: (
            "999" if conflict and kwargs["id_type"] == "mal" else None
        )
    )
    client = SimpleNamespace(fetch_anime=AsyncMock())
    with caplog.at_level(logging.DEBUG, logger="betterer-ratings"):
        result = asyncio.run(
            fetch_candidate_mal_score(
                client=client,
                db=db,
                candidate=SimpleNamespace(tmdb_id=128, media_type="movie"),
                details={},
                md_item={"ids": {"mal": "164"}} if conflict else {},
                stop_event=asyncio.Event(),
            )
        )
    event = caplog.records[-1]
    assert event.event == "mal.enrichment"
    assert event.tmdb_id == 128
    assert (
        event.outcome == result.outcome == ("mapping_conflict" if conflict else "missing_mapping")
    )
    assert event.levelno == (logging.WARNING if conflict else logging.DEBUG)
    if conflict:
        assert event.conflicts == [{"provider": "mal", "source_id": "164", "stored_id": "999"}]
    client.fetch_anime.assert_not_awaited()
