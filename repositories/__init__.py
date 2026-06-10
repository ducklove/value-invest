"""Per-table data-access repositories extracted from cache.py.

Each module owns the SQL for one table group. ``cache.py`` re-exports these
functions at the bottom of the module so existing ``cache.<fn>`` call sites stay
unchanged while the data layer is split into cohesive units. Repositories reach
the shared connection via ``repositories.db`` (connection singleton +
``transaction()``); no repository depends on cache.py anymore, so the old
circular coupling is gone.
"""
