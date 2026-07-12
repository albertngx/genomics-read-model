"""cdc_pipeline — CDC-driven read models.

One package, three subpackages:
- ``api``        the source-of-truth tracking service (epic 02)
- ``consumers``  the Kafka consumers that build the read models (epics 04–05)
- ``tools``      operational CLIs: verifier, replay, demos (epic 06)

Configuration (connection strings, ports) is never hardcoded — it is read from the
environment via :class:`cdc_pipeline.config.Settings`, whose single source of truth
is ``.env.example``.
"""

__version__ = "0.1.0"
