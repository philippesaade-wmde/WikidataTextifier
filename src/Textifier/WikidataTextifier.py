"""Backward-compatible aliases for the renamed textifier module.

This module preserves imports like
``from src.Textifier.WikidataTextifier import WikidataEntity`` while the
canonical implementation lives in ``src.Textifier.WikibaseTextifier``.
"""

from .WikibaseTextifier import (
    LANGUAGE_VARIABLES,
    LANGUAGE_VARIABLES_PATH,
    WikibaseClaim,
    WikibaseClaimValue,
    WikibaseCoordinates,
    WikibaseEntity,
    WikibaseMonolingualText,
    WikibaseQuantity,
    WikibaseText,
    WikibaseTime,
)

# Backward compatibility aliases.
WikidataText = WikibaseText
WikidataMonolingualText = WikibaseMonolingualText
WikidataCoordinates = WikibaseCoordinates
WikidataTime = WikibaseTime
WikidataQuantity = WikibaseQuantity
WikidataEntity = WikibaseEntity
WikidataClaim = WikibaseClaim
WikidataClaimValue = WikibaseClaimValue

__all__ = [
    "LANGUAGE_VARIABLES",
    "LANGUAGE_VARIABLES_PATH",
    "WikibaseClaim",
    "WikibaseClaimValue",
    "WikibaseCoordinates",
    "WikibaseEntity",
    "WikibaseMonolingualText",
    "WikibaseQuantity",
    "WikibaseText",
    "WikibaseTime",
    "WikidataClaim",
    "WikidataClaimValue",
    "WikidataCoordinates",
    "WikidataEntity",
    "WikidataMonolingualText",
    "WikidataQuantity",
    "WikidataText",
    "WikidataTime",
]
