from __future__ import annotations

from typing import Any, Callable, Optional, Sequence, Set

from betterer_ratings.core.clock import format_duration


async def submit_mapping_group(
    *,
    rows: Sequence[Any],
    pmdb_client: Any,
    db: Any,
    submit_mapping_fn: Callable[..., Any],
    now_epoch_fn: Callable[[], int],
    logger: Any,
) -> None:
    """Resolve all existing mappings for a title with one PMDB lookup."""
    rows = [row for row in rows if not db.defer_held_mapping(row)]
    if not rows:
        return

    first = rows[0]
    tmdb_id = int(first["tmdb_id"])
    media_type = str(first["media_type"])
    lookup = await pmdb_client._fetch_existing_mappings(tmdb_id, media_type)

    unresolved = list(rows)
    resolved_count = 0
    if lookup.status == 200 and isinstance(lookup.data, dict):
        submitted_at = now_epoch_fn()
        for row in list(unresolved):
            if db.defer_held_mapping(row):
                unresolved.remove(row)
                continue
            id_type = str(row["id_type"])
            id_value = str(row["id_value"])
            entries = pmdb_client._extract_mappings_for_type(lookup.data, id_type)
            matching_entry = next(
                (
                    entry
                    for entry in entries
                    if pmdb_client._mapping_entry_matches_value(entry, id_value)
                ),
                None,
            )
            if matching_entry is None:
                continue
            db.mark_mapping_submitted(
                tmdb_id,
                media_type,
                id_type,
                submitted_at,
            )
            unresolved.remove(row)
            resolved_count += 1

    if resolved_count:
        logger.info(
            "[Submitter] Mapping preflight resolved existing: %s %s resolved=%s missing=%s",
            media_type,
            tmdb_id,
            resolved_count,
            len(unresolved),
            extra={
                "event": "mapping.preflight_resolved",
                "entity": "mapping",
                "media_type": media_type,
                "tmdb_id": tmdb_id,
                "resolved": resolved_count,
                "missing": len(unresolved),
            },
        )

    for row in unresolved:
        if not db.defer_held_mapping(row):
            await submit_mapping_fn(row=row)


async def submit_mapping(
    *,
    row: Any,
    pmdb_client: Any,
    db: Any,
    verify_after_transient_statuses: Set[int],
    max_retry_attempts: int,
    format_manual_error_fn: Callable[..., str],
    format_pmdb_error_fn: Callable[..., str],
    retry_delay_seconds_fn: Callable[[Any, int], int],
    now_epoch_fn: Callable[[], int],
    first_non_empty_fn: Callable[..., Optional[str]],
    logger: Any,
) -> None:
    if db.defer_held_mapping(row):
        return
    tmdb_id = int(row["tmdb_id"])
    media_type = str(row["media_type"])
    id_type = str(row["id_type"])
    id_value = str(row["id_value"])
    pmdb_item_id = first_non_empty_fn(row["pmdb_item_id"])
    attempts = int(row["pmdb_attempts"] or 0)

    result = await pmdb_client.submit_mapping(
        tmdb_id=tmdb_id,
        media_type=media_type,
        id_type=id_type,
        id_value=id_value,
    )
    result_context = {
        "entity": "mapping", "provider": "pmdb", "tmdb_id": tmdb_id,
        "media_type": media_type, "id_type": id_type, "id_value": id_value,
        "status": result.status_code, "error_code": result.error_code,
        "endpoint": result.endpoint, "attempt": attempts + 1,
        "max_attempts": max_retry_attempts,
    }

    if result.success:
        db.mark_mapping_submitted(
            tmdb_id,
            media_type,
            id_type,
            now_epoch_fn(),
            pmdb_item_id=result.item_id,
        )
        if result.duplicate_or_exists:
            logger.info(
                "[Submitter] Mapping already exists: %s %s %s=%s",
                media_type,
                tmdb_id,
                id_type,
                id_value,
                extra={
                    "event": "mapping.exists",
                    "entity": "mapping",
                    "media_type": media_type,
                    "tmdb_id": tmdb_id,
                    "id_type": id_type,
                },
            )
        else:
            logger.info(
                "[Submitter] Mapping submitted: %s %s %s=%s",
                media_type,
                tmdb_id,
                id_type,
                id_value,
                extra={
                    "event": "mapping.submitted",
                    "entity": "mapping",
                    "media_type": media_type,
                    "tmdb_id": tmdb_id,
                    "id_type": id_type,
                },
            )
        return

    if result.retryable:
        if int(result.status_code or 0) in verify_after_transient_statuses:
            # `confirm_mapping_exists` matches by id_type+id_value against an
            # unauthenticated lookup, so a hit is not proof the entry is
            # ours — only cache ids that came back from our own create
            # response, never from this confirmation match.
            found_existing, _found_item_id = await pmdb_client.confirm_mapping_exists(
                tmdb_id=tmdb_id,
                media_type=media_type,
                id_type=id_type,
                id_value=id_value,
            )
            if found_existing:
                db.mark_mapping_submitted(
                    tmdb_id,
                    media_type,
                    id_type,
                    now_epoch_fn(),
                    pmdb_item_id=pmdb_item_id,
                )
                logger.info(
                    "[Submitter] Mapping confirmed remote after transient failure: %s %s %s=%s",
                    media_type,
                    tmdb_id,
                    id_type,
                    id_value,
                    extra={**result_context, "event": "mapping.confirmed"},
                )
                return

        if attempts + 1 >= max_retry_attempts:
            db.mark_mapping_failed(
                tmdb_id,
                media_type,
                id_type,
                format_manual_error_fn(
                    endpoint=result.endpoint or "/api/external/mappings",
                    status=int(result.status_code or 0),
                    code="max_retry_attempts_exceeded",
                    retryable=False,
                    message=(
                        f"Exceeded max retry attempts ({max_retry_attempts}) "
                        f"for mapping submission."
                    ),
                ),
            )
            logger.error(
                "[Submitter] Mapping failed permanently after max attempts (%s): %s %s %s=%s",
                max_retry_attempts,
                media_type,
                tmdb_id,
                id_type,
                id_value,
                extra={**result_context, "event": "mapping.failed", "reason": "max_attempts", "error": result.error_text[:500]},
            )
            return

        retry_delay = retry_delay_seconds_fn(result, attempts)
        retry_at = now_epoch_fn() + retry_delay
        error_payload = format_pmdb_error_fn(
            endpoint_hint="/api/external/mappings",
            result=result,
        )
        db.mark_mapping_retry(
            tmdb_id,
            media_type,
            id_type,
            retry_at,
            error_payload,
        )
        logger.warning(
            "[Submitter] Mapping retry scheduled in %s: %s %s %s=%s",
            format_duration(retry_delay),
            media_type,
            tmdb_id,
            id_type,
            id_value,
            extra={**result_context, "event": "mapping.retry_scheduled", "retry_at": retry_at, "backoff_seconds": retry_delay, "error": result.error_text[:500]},
        )
        return

    db.mark_mapping_failed(
        tmdb_id,
        media_type,
        id_type,
        format_pmdb_error_fn(
            endpoint_hint="/api/external/mappings",
            result=result,
        ),
    )
    logger.error(
        "[Submitter] Mapping failed permanently: %s %s %s=%s (%s)",
        media_type,
        tmdb_id,
        id_type,
        id_value,
        result.error_text[:200],
        extra={**result_context, "event": "mapping.failed", "reason": "non_retryable", "error": result.error_text[:500]},
    )
