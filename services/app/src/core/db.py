from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .config import get_settings


def make_async_engine() -> AsyncEngine:
    return create_async_engine(get_settings().database_url, pool_size=5, max_overflow=2)


def make_sync_engine() -> Engine:
    return create_engine(get_settings().sync_database_url)
