from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import settings

engine = create_engine(
    settings.db_url,
    pool_size=settings.pool_size,
    max_overflow=settings.max_overflow,
    pool_timeout=settings.pool_timeout,
    # PostgreSQL session-level statement_timeout（毫秒），防 lock wait 掛住 thread
    connect_args={"options": f"-c statement_timeout={settings.statement_timeout_ms}"},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()