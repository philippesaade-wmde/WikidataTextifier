"""Unit tests for label cache helpers.

Covers language fallback behavior, nested ID extraction, and lazy label resolution.
"""

from src.WikibaseLabel import LazyLabelFactory, WikibaseLabel


def test_get_lang_val_prefers_requested_language():
    """It should prefer the exact requested language when present."""
    labels = {
        "en": {"value": "Douglas Adams"},
        "mul": {"value": "Douglas Adams (multilingual)"},
    }

    assert WikibaseLabel.get_lang_val(labels, lang="en", fallback_lang="fr") == "Douglas Adams"


def test_get_lang_val_falls_back_to_mul_and_fallback_language():
    """It should use ``mul`` first, then explicit fallback when needed."""
    labels_with_mul = {
        "mul": {"value": "Universal label"},
        "fr": {"value": "Etiquette"},
    }
    labels_without_mul = {
        "fr": {"value": "Etiquette"},
    }

    assert WikibaseLabel.get_lang_val(labels_with_mul, lang="en", fallback_lang="fr") == "Universal label"
    assert WikibaseLabel.get_lang_val(labels_without_mul, lang="en", fallback_lang="fr") == "Etiquette"


def test_get_lang_val_falls_back_to_any_available_language():
    """It should select a stable available language with the wildcard fallback."""
    labels = {
        "de": {"value": "Etikett"},
        "fr": {"value": "Etiquette"},
    }

    assert WikibaseLabel.get_lang_val(labels, lang="en", fallback_lang="any") == "Etikett"


def test_get_all_missing_labels_ids_collects_nested_ids():
    """It should collect IDs from nested property, unit, claim, and datavalue branches."""
    payload = {
        "property": "P31",
        "unit": "https://example.wikibase.local/entity/Q11573",
        "datatype": "wikibase-item",
        "datavalue": {"value": {"id": "Q5"}},
        "claims": {"P279": []},
        "nested": [{"property": "P17"}],
    }

    ids = WikibaseLabel.get_all_missing_labels_ids(payload)

    assert ids == {"P31", "Q11573", "Q5", "P279", "P17"}


def test_lazy_label_factory_resolves_pending_labels_in_bulk(monkeypatch):
    """It should resolve pending IDs via a single bulk lookup when cast to ``str``."""

    def fake_get_bulk_labels(ids, wb_url="https://example.wikibase.local"):
        del wb_url
        return {"Q42": {"en": "Douglas Adams"}}

    monkeypatch.setattr(WikibaseLabel, "get_bulk_labels", staticmethod(fake_get_bulk_labels))

    factory = LazyLabelFactory(lang="en", fallback_lang="en")
    lazy_label = factory.create("Q42")

    assert str(lazy_label) == "Douglas Adams"


def test_lazy_label_factory_forwards_wikibase_url(monkeypatch):
    """It should forward the configured Wikibase URL to bulk label lookups."""
    calls = []

    def fake_get_bulk_labels(ids, wb_url="https://example.wikibase.local"):
        calls.append((list(ids), wb_url))
        return {"Q42": {"en": "Douglas Adams"}}

    monkeypatch.setattr(WikibaseLabel, "get_bulk_labels", staticmethod(fake_get_bulk_labels))

    factory = LazyLabelFactory(lang="en", fallback_lang="en", wb_url="https://example.wikibase.local/")
    lazy_label = factory.create("Q42")

    assert str(lazy_label) == "Douglas Adams"
    assert calls == [(["Q42"], "https://example.wikibase.local")]
