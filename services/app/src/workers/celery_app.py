import os

from celery import Celery

celery_app = Celery(
    "macro",
    broker=os.environ["REDIS_URL"],
    backend=os.environ["REDIS_URL"],
    include=["workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # Tasks should auto-retry on transient errors at the framework level too;
    # individual ingestors have their own tenacity wrapper for the network call.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)

# Load beat schedule
from workers.beat_schedule import beat_schedule  # noqa: E402

celery_app.conf.beat_schedule = beat_schedule


@celery_app.task(name="workers.ping")
def ping() -> str:
    return "pong"
