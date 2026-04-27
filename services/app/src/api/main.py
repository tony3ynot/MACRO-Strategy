import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DB_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["REDIS_URL"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.engine = create_async_engine(DB_URL, pool_size=5, max_overflow=2)
    app.state.redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield
    finally:
        await app.state.engine.dispose()
        await app.state.redis.aclose()


app = FastAPI(title="MACRO Strategy", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health():
    db_ok = False
    db_tsdb = None
    redis_ok = False

    try:
        async with app.state.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("SELECT extversion FROM pg_extension WHERE extname='timescaledb'")
                )
            ).scalar_one_or_none()
            db_ok = True
            db_tsdb = row
    except Exception:
        pass

    try:
        await app.state.redis.ping()
        redis_ok = True
    except Exception:
        pass

    status = "ok" if (db_ok and redis_ok) else "degraded"
    return {
        "status": status,
        "db": db_ok,
        "timescaledb": db_tsdb,
        "redis": redis_ok,
    }
