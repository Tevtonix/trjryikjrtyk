# ... существующие импорты ...
from app.jobs.handlers.word_stats_compare import (
    validate_word_stats_compare_payload,
    compare_word_stats_results,
)

# В функции обработки задачи (обычно в dispatch или handle_job):
async def execute_job(job: JobDB, session):  # или как у вас называется
    if job.job_type == JobType.WORD_STATS_COMPARE:
        try:
            left_id, right_id = validate_word_stats_compare_payload(job.payload)

            # Загружаем обе задачи
            left_job = await get_job_by_id(session, left_id)   # ваша функция получения задачи
            right_job = await get_job_by_id(session, right_id)

            # Сравниваем
            result_data = compare_word_stats_results(left_job, right_job)

            # Сохраняем результат
            job.result = result_data
            job.status = "DONE"
            await session.commit()
            await session.refresh(job)

            return

        except PermanentJobError as e:
            job.status = "FAILED"
            job.error = str(e)
            await session.commit()
            raise
        except Exception as e:  # другие ошибки
            job.status = "FAILED"
            job.error = f"Unexpected error: {str(e)}"
            await session.commit()
            raise

    # ... обработка остальных типов задач (WORD_STATS и т.д.) ...