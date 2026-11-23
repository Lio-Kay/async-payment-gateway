import logging
from decimal import Decimal

import pytest
from alembic.config import Config
from sqlalchemy import CheckConstraint, Numeric, inspect
from sqlalchemy.dialects.postgresql import JSONB

from alembic import command
from src.core.logging import JsonFormatter, setup_logging
from src.models import Base, Currency, OutboxEvent, OutboxStatus, Payment, PaymentStatus


def test_enums_match_contract() -> None:
    assert [c.value for c in Currency] == ["RUB", "USD", "EUR"]
    assert [s.value for s in PaymentStatus] == ["pending", "succeeded", "failed"]
    assert [s.value for s in OutboxStatus] == ["pending", "published", "failed"]


def test_payment_model_schema() -> None:
    assert Payment.__tablename__ == "payments"
    mapper = inspect(Payment)
    table = Payment.__table__

    expected_columns = {
        "id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "idempotency_key",
        "webhook_url",
        "created_at",
        "processed_at",
    }
    assert set(table.columns.keys()) == expected_columns

    # Python attribute `payment_metadata` maps to DB column `"metadata"` (JSONB)
    assert mapper.attrs["payment_metadata"].columns[0].name == "metadata"
    assert isinstance(table.c.metadata.type, JSONB)

    # PostgreSQL enum columns must store lowercase `.value`, not uppercase `.name`
    assert table.c.currency.type.enums == ["RUB", "USD", "EUR"]
    assert table.c.status.type.enums == ["pending", "succeeded", "failed"]

    # Amount precision/scale and positive check constraint
    assert isinstance(table.c.amount.type, Numeric)
    assert table.c.amount.type.precision == 12
    assert table.c.amount.type.scale == 2
    check_constraints = [c for c in table.constraints if isinstance(c, CheckConstraint)]
    assert any("amount > 0" in str(c.sqltext) for c in check_constraints)

    # Idempotency key is unique and indexed
    assert table.c.idempotency_key.unique is True
    assert table.c.idempotency_key.index is True

    payment = Payment(
        amount=Decimal("100.50"),
        currency=Currency.USD,
        description="Order #1",
        payment_metadata={"order_id": "1"},
        idempotency_key="idem-1",
        webhook_url="https://example.com/webhook",
    )
    assert payment.payment_metadata == {"order_id": "1"}


def test_outbox_model_schema() -> None:
    assert OutboxEvent.__tablename__ == "outbox"
    table = OutboxEvent.__table__

    expected_columns = {
        "id",
        "aggregate_id",
        "event_type",
        "payload",
        "status",
        "retry_count",
        "created_at",
        "processed_at",
    }
    assert set(table.columns.keys()) == expected_columns
    assert isinstance(table.c.payload.type, JSONB)
    assert table.c.status.type.enums == ["pending", "published", "failed"]

    # Composite index for FOR UPDATE SKIP LOCKED polling
    index_names = {idx.name for idx in table.indexes}
    assert "ix_outbox_status_created_at" in index_names

    # Foreign key to payments.id with ON DELETE CASCADE
    fks = list(table.c.aggregate_id.foreign_keys)
    assert len(fks) == 1
    assert fks[0].target_fullname == "payments.id"
    assert fks[0].ondelete == "CASCADE"

    assert set(Base.metadata.tables.keys()) == {"payments", "outbox"}


def test_alembic_offline_upgrade_and_downgrade_sql(
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_logging("INFO")
    alembic_cfg = Config("alembic.ini")

    command.upgrade(alembic_cfg, "head", sql=True)
    upgrade_sql = capsys.readouterr().out
    assert "CREATE TYPE currency_enum AS ENUM ('RUB', 'USD', 'EUR')" in upgrade_sql
    assert (
        "CREATE TYPE payment_status_enum AS ENUM ('pending', 'succeeded', 'failed')" in upgrade_sql
    )
    assert (
        "CREATE TYPE outbox_status_enum AS ENUM ('pending', 'published', 'failed')" in upgrade_sql
    )
    assert "CREATE TABLE payments" in upgrade_sql
    assert "CREATE TABLE outbox" in upgrade_sql
    assert "CREATE UNIQUE INDEX ix_payments_idempotency_key" in upgrade_sql
    assert "CREATE INDEX ix_outbox_status_created_at" in upgrade_sql

    command.downgrade(alembic_cfg, "20261122_0001:base", sql=True)
    downgrade_sql = capsys.readouterr().out
    assert "DROP TABLE outbox" in downgrade_sql
    assert "DROP TABLE payments" in downgrade_sql
    assert "DROP TYPE outbox_status_enum" in downgrade_sql
    assert "DROP TYPE payment_status_enum" in downgrade_sql
    assert "DROP TYPE currency_enum" in downgrade_sql

    # Verify running Alembic did not clobber our JSON root logger handler.
    root_logger = logging.getLogger()
    assert len(root_logger.handlers) == 1
    assert isinstance(root_logger.handlers[0].formatter, JsonFormatter)


def test_orm_metadata_names_match_initial_migration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure constraint and index names on Base.metadata match the migration DDL."""
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head", sql=True)
    upgrade_sql = capsys.readouterr().out

    for table in Base.metadata.tables.values():
        for constraint in table.constraints:
            if constraint.name:
                assert constraint.name in upgrade_sql
        for index in table.indexes:
            assert index.name in upgrade_sql
