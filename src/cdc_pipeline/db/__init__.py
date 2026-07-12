"""Source-of-truth database layer: the ``runs``/``specimens`` schema, its Alembic
migrations (which also own the CDC capture configuration — replica identity and the
Debezium publication), and the idempotent seed.

The SQLAlchemy models here are shared: the migrations are authoritative for the DDL,
but the models back the seed (this story) and the tracking API (epic 02.2).
"""
