import os

from celery import Celery

celery_app = Celery(
    "macro",
    broker=os.environ["REDIS_URL"],
    backend=os.environ["REDIS_URL"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone=os.environ.get("TZ", "UTC"),
    enable_utc=True,
)


@celery_app.task(name="workers.ping")
def ping() -> str:
    return "pong"
