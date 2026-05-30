"""Per-table data-access repositories extracted from cache.py.

Each module owns the SQL for one table group. ``cache.py`` re-exports these
functions at the bottom of the module so existing ``cache.<fn>`` call sites stay
unchanged while the data layer is split into cohesive units. Repositories reach
the shared connection via ``cache.get_db()`` (imported lazily as a module, so
there is no import cycle with cache.py's bottom-of-file re-exports).
"""
