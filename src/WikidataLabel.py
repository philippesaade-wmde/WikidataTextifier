"""Backward-compatible aliases for the renamed Wikibase label helpers."""

from .WikibaseLabel import (
    DATABASE_URL,
    DB_HOST,
    DB_NAME,
    DB_PASS,
    DB_PORT,
    DB_USER,
    DEFAULT_WIKIBASE_URL,
    LABEL_MAX_ROWS,
    LABEL_TTL_DAYS,
    LABEL_UNLIMITED,
    REQUEST_TIMEOUT_SECONDS,
    Base,
    LazyLabel,
    LazyLabelFactory,
    Session,
    WikibaseLabel,
    engine,
)

# Backward compatibility alias: external callers may still import WikidataLabel.
WikidataLabel = WikibaseLabel

__all__ = [
    "Base",
    "DATABASE_URL",
    "DB_HOST",
    "DB_NAME",
    "DB_PASS",
    "DB_PORT",
    "DB_USER",
    "DEFAULT_WIKIBASE_URL",
    "LABEL_MAX_ROWS",
    "LABEL_TTL_DAYS",
    "LABEL_UNLIMITED",
    "REQUEST_TIMEOUT_SECONDS",
    "Session",
    "WikibaseLabel",
    "WikidataLabel",
    "LazyLabel",
    "LazyLabelFactory",
    "engine",
]
