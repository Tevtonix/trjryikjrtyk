from uuid import UUID
from typing import Any, Dict

from app.jobs.models import JobDB
from app.jobs.exceptions import PermanentJobError


def validate_word_stats_compare_payload(payload: Dict[str, Any] | None) -> tuple[UUID, UUID]:
    """Валидация payload для задачи WORD_STATS_COMPARE."""
    if not payload:
        raise PermanentJobError("WORD_STATS_COMPARE requires payload")

    left_job_id = payload.get("left_job_id")
    right_job_id = payload.get("right_job_id")

    if not left_job_id or not right_job_id:
        raise PermanentJobError("WORD_STATS_COMPARE requires both left_job_id and right_job_id in payload")

    try:
        left_uuid = UUID(str(left_job_id))
        right_uuid = UUID(str(right_job_id))
    except (ValueError, TypeError):
        raise PermanentJobError("left_job_id and right_job_id must be valid UUIDs")

    if left_uuid == right_uuid:
        raise PermanentJobError("left_job_id and right_job_id must be different")

    return left_uuid, right_uuid


def compare_word_stats_results(
    left_job: JobDB,
    right_job: JobDB,
) -> Dict[str, Any]:
    """Сравнивает результаты двух задач WORD_STATS и возвращает итоговый result."""

    # Проверка корректности задач (выполняется до вызова этой функции, но дублируем для безопасности)
    def validate_source_job(job: JobDB, side: str) -> None:
        if job is None:
            raise PermanentJobError(f"{side.capitalize()} source job not found")
        if job.job_type != "WORD_STATS":
            raise PermanentJobError(f"{side.capitalize()} source job must have type WORD_STATS")
        if job.status != "DONE":
            raise PermanentJobError(f"{side.capitalize()} source job must be in DONE status")
        if not isinstance(job.result, dict):
            raise PermanentJobError(f"{side.capitalize()} source job result must be a dict")
        if "top_words" not in job.result or not isinstance(job.result["top_words"], dict):
            raise PermanentJobError(f"{side.capitalize()} source job result must contain 'top_words' dict")

    validate_source_job(left_job, "left")
    validate_source_job(right_job, "right")

    left_top = left_job.result["top_words"]
    right_top = right_job.result["top_words"]

    # Множества слов
    left_words = set(left_top.keys())
    right_words = set(right_top.keys())

    common_words = sorted(left_words & right_words)
    left_only = sorted(left_words - right_words)
    right_only = sorted(right_words - left_words)

    return {
        "left_job_id": str(left_job.id),
        "right_job_id": str(right_job.id),
        "left_url": left_job.payload.get("url", ""),   # предполагаем, что в payload WORD_STATS есть url
        "right_url": right_job.payload.get("url", ""),
        "common_words": common_words,
        "left_only": left_only,
        "right_only": right_only,
    }