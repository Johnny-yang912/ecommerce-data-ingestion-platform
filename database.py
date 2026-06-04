from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from config import settings

# 約束/索引命名慣例：掛在 Base.metadata 上，讓 Alembic autogenerate 產生
# 穩定、可預期的名稱（而非 DB 自動命名），未來 drop/rename 不會因環境命名不一致而出錯。
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referent_table_name)s",
    "pk": "pk_%(table_name)s",
}

engine = create_engine(
    settings.db_url,
    pool_size=settings.pool_size,
    max_overflow=settings.max_overflow,
    pool_timeout=settings.pool_timeout,
    # PostgreSQL session-level statement_timeout（毫秒），防 lock wait 掛住 thread
    connect_args={"options": f"-c statement_timeout={settings.statement_timeout_ms}"},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base(metadata=MetaData(naming_convention=NAMING_CONVENTION))