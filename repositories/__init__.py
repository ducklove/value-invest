"""Per-table data-access repositories.

Each module owns the SQL for one table group. Routes/services import the
repository they need directly (the old ``cache.py`` god-module and its
re-export shim were dismantled in Phase 2). Repositories reach the shared
connection via ``repositories.db`` (connection singleton + ``transaction()``);
schema creation and one-time backfills are orchestrated by
``repositories.bootstrap.init_db``.
"""
