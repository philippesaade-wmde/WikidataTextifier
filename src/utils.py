from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from babel import Locale
from babel.dates import format_date, format_time
from babel.units import format_unit
from babel.numbers import format_decimal
import re
import requests
from .WikidataLabel import WikidataLabel

def get_wikidata_entity_by_id(
        id,
        props='labels,descriptions,aliases,statements'
    ):
    """
    Fetches a Wikidata entity by its ID and returns a dictionary of the entity.

    Parameters:
    - id (str): A Wikidata entity ID (e.g., Q42, P31).
    - props (str): The properties to retrieve.

    Returns:
    - dict: A dictionary containing the entity, where keys are entity IDs and values are dictionaries of properties.
    """
    entity_type = 'items'
    if id.startswith('P'):
        entity_type = 'properties'

    params = {
        '_fields': props,
    }
    headers = {
        'User-Agent': 'Wikidata Textifier'
    }

    response = requests.get(
        f"https://www.wikidata.org/w/rest.php/wikibase/v1/entities/{entity_type}/{id}",
        params=params,
        headers=headers
    )
    response.raise_for_status()
    entity_data = response.json()
    return entity_data


def get_wikidata_entities_by_ids(
        ids,
        props='labels|descriptions|aliases|claims'
    ):
    """
    Fetches Wikidata entities by their IDs and returns a dictionary of entities.

    Parameters:
    - ids (list[str] or str): A list of Wikidata entity IDs (e.g., Q42, P31) or a single ID as a string.
    - props (str): The properties to retrieve (default is 'labels|descriptions|aliases|claims').

    Returns:
    - dict: A dictionary containing the entities, where keys are entity IDs and values are dictionaries of properties.
    """

    if isinstance(ids, str):
        ids = ids.split('|')
    ids = list(set(ids)) # Ensure unique IDs

    entities_data = {}

    # Wikidata API has a limit on the number of IDs per request,
    # typically 50 for wbgetentities.
    for chunk_idx in range(0, len(ids), 50):

        ids_chunk = ids[chunk_idx:chunk_idx+50]
        params = {
            'action': 'wbgetentities',
            'ids': "|".join(ids_chunk),
            'props': props,
            'format': 'json',
            'origin': '*',
        }
        headers = {
            'User-Agent': 'Wikidata Textifier'
        }

        response = requests.get(
            "https://www.wikidata.org/w/api.php?",
            params=params,
            headers=headers
        )
        response.raise_for_status()
        chunk_data = response.json().get("entities", {})
        entities_data = entities_data | chunk_data

    return entities_data


def get_all_missing_labels_ids(data):
    """
    Get the IDs of the entity dictionary where their labels are missing.

    Parameters:
    - data (dict or list): The data structure to search for missing labels.

    Returns:
    - set: A set of IDs that are missing labels.
    """
    ids_list = set()

    if isinstance(data, dict):
        if 'property' in data:
            ids_list.add(data['property'])
        if ('unit' in data) and (data['unit'] != '1'):
            ids_list.add(data['unit'].split('/')[-1])
        if ('datatype' in data) and \
            ('datavalue' in data) and \
            (data['datatype'] in ['wikibase-item', 'wikibase-property']):
            ids_list.add(data['datavalue']['value']['id'])

        for _, value in data.items():
            ids_list = ids_list | get_all_missing_labels_ids(value)

    elif isinstance(data, list):
        for item in data:
            ids_list = ids_list | get_all_missing_labels_ids(item)

    return ids_list


#####################################
# Time Formatting
#####################################

def _parse_iso(iso: str):
    ISO_RX = re.compile(r'^(?P<sign>[+-])(?P<year>\d{4,16})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})Z$')
    m = ISO_RX.match(iso)
    if not m:
        raise ValueError(f"Bad ISO time: {iso}")
    sign = -1 if m.group("sign") == "-" else 1
    return (sign * int(m.group("year")), int(m.group("month")), int(m.group("day")),
            int(m.group("hour")), int(m.group("minute")), int(m.group("second")))

def _astronomical_to_human_era(year: int, loc):
    """Convert astronomical year to human year with era (0->1 BCE, -1->2 BCE, 1->1 CE)"""
    eras = getattr(loc, "eras", None) or {}
    labels = eras.get("abbreviated") or eras.get("wide") or {0:"BC", 1:"AD"}
    return (1 - year, labels.get(0, "BC")) if year <= 0 else (year, None)

def get_temporal_label(unit_key: str, lang: str, fallback_lang: str = None):
    UNIT_QIDS = {
        "year": "Q577", "decade": "Q39911", "century": "Q578", "millennium": "Q36507",
        "hundred_thousand_years": "Q24004476", "million_years": "Q24004475", "billion_years": "Q24004466",
    }

    qid = UNIT_QIDS.get(unit_key) or (unit_key if re.fullmatch(r"Q[1-9]\d*", unit_key) else None)
    if not qid:
        return None
    labels = WikidataLabel.get_labels(qid)
    return WikidataLabel.get_lang_val(labels, lang=lang, fallback_lang=fallback_lang)

def _range_label(loc: Locale, y_start: int, y_end: int):
    hs, es = _astronomical_to_human_era(y_start, loc)
    he, ee = _astronomical_to_human_era(y_end, loc)

    if (es or ee) and (es != ee):
        left = f"{hs} {es}" if es else str(hs)
        right = f"{he} {ee}" if ee else str(he)
        return f"{left}–{right}"

    if es:
        return f"{hs}–{he} {es}"
    return f"{hs}–{he}"

def _bucket_bounds_astrological(y: int, precision: int):
    human, is_bce = (1 - y, True) if y <= 0 else (y, False)
    width = {8: 10, 7: 100, 6: 1000}[precision]
    base = (human // width) * width
    low_h, high_h = base, base + width - 1

    if is_bce:
        return 1 - high_h, 1 - low_h
    return low_h, high_h

def _format_geologic(loc, abs_year: int, lang: str):
    """Format deep time (precision 0..5) with composite units or plain years + BCE"""
    bce = (getattr(loc, "eras", None) or {}).get("abbreviated", {}).get(0, "BC")

    # Check for exact multiples of scale factors
    scales = [(1_000_000_000, "billion_years"), (1_000_000, "million_years"), (100_000, "hundred_thousand_years")]

    for factor, key in scales:
        if abs_year % factor == 0:
            count = abs_year // factor

            # Special case: exactly 100,000 → prefer "100,000 years BCE"
            if factor == 100_000 and count == 1:
                return _format_years_with_cldr_or_wd(loc, abs_year, lang, bce)

            # Use composite unit label if available
            unit_label = get_temporal_label(key, lang)
            if unit_label:
                return f"{format_decimal(count, locale=loc)} {unit_label} {bce}"
            break

    # Fallback to plain years formatting
    return _format_years_with_cldr_or_wd(loc, abs_year, lang, bce)

def _format_years_with_cldr_or_wd(loc, abs_year: int, lang: str, bce: str):
    """Helper to format years using CLDR or Wikidata labels"""
    unit_names = loc.unit_display_names.get('duration-year', {})

    if unit_names.get('long'):
        cldr_phrase = format_unit(abs_year, "year", locale=str(loc), length='long')
    elif unit_names.get('short'):
        cldr_phrase = format_unit(abs_year, "year", locale=str(loc), length='short')
    else:
        wd_year = get_temporal_label("year", lang)
        num_str = format_decimal(abs_year, locale=loc)
        unit_word = wd_year or ("year" if abs_year == 1 else "years")
        return f"{num_str} {unit_word} {bce}"

    return f"{cldr_phrase} {bce}"

def _format_point(loc, dt: datetime, precision: int):
    """Format a point in time based on precision"""
    if precision >= 12:
        date_str = format_date(dt, format="long", locale=loc)
        time_formats = {14: "HH:mm:ss", 13: "HH:mm", 12: "HH"}
        time_str = format_time(dt, format=time_formats[precision], locale=loc)
        return f"{date_str} {time_str}"

    if precision == 11:
        return format_date(dt, format="long", locale=loc)

    if precision == 10:
        return format_date(dt, format="MMMM y", locale=loc)

    if precision == 9:
        return str(dt.year if dt.year > 0 else 1)

    return ""

def wikidata_time_to_text(value: dict, lang: str = "en"):
    """
    Convert a Wikidata time value into natural language text.

    - precision 0..5: deep time with composite units or plain years + BCE
    - precision 6..8: numeric ranges (millennium/century/decade), BCE-safe
    - precision 9..11: year/month/day via CLDR patterns
    - precision 12..14: date + time with optional UTC offset
    """
    try:
        loc = Locale.parse(lang)
    except Exception:
        loc = Locale.parse("en")

    # Parse values
    y, m, d, H, M, S = _parse_iso(value["time"])
    tzmin = int(value.get("timezone", 0) or 0)
    before = int(value.get("before", 0) or 0)
    after = int(value.get("after", 0) or 0)
    precision = int(value.get("precision", 11) or 11)
    calendarmodel = value.get("calendarmodel", "").rsplit("/", 1)[-1]

    # Create base datetime
    base = datetime(min(max(abs(y), 1), 9999), max(m, 1), max(d, 1), H, M, S)
    base = base.replace(tzinfo=timezone(timedelta(minutes=tzmin)))

    # Deep time formatting
    if precision <= 5:
        return _format_geologic(loc, abs(y), lang)

    # Range formatting for decades/centuries/millennia
    if precision in (8, 7, 6):
        a, b = _bucket_bounds_astrological(y, precision)

        # Adjust for before/after if specified
        if before or after:
            width = {8: 10, 7: 100, 6: 1000}[precision]
            human, is_bce = (1 - y, True) if y <= 0 else (y, False)
            low_h = (human // width) * width - after * width
            high_h = (human // width) * width + (width - 1) + before * width
            a, b = (1 - high_h, 1 - low_h) if is_bce else (low_h, high_h)

        label = _range_label(loc, a, b)
    else:
        # Point or range formatting for years/months/days/times
        if before or after:
            DELTA_BY_PREC = {
                14: relativedelta(seconds=1), 13: relativedelta(minutes=1), 12: relativedelta(hours=1),
                11: relativedelta(days=1), 10: relativedelta(months=1), 9: relativedelta(years=1),
                8: relativedelta(years=10), 7: relativedelta(years=100), 6: relativedelta(years=1000),
            }
            delta = DELTA_BY_PREC[precision]
            start = base - (delta * after) if after else base
            end = base + (delta * before) if before else base
            label = f"{_format_point(loc, start, precision)}–{_format_point(loc, end, precision)}"
        else:
            label = _format_point(loc, base, precision)

        # Add timezone offset if applicable
        if precision >= 12 and tzmin != 0:
            sign = "+" if tzmin >= 0 else "-"
            hh, mm = divmod(abs(tzmin), 60)
            label = f"{label} UTC{sign}{hh:02d}:{mm:02d}"

    # Add calendar label for non-Gregorian calendars
    if calendarmodel != "Q1985727":
        cal_label = get_temporal_label(calendarmodel, lang, fallback_lang='en')
        if cal_label:
            label = f"{label} ({cal_label})"

    return label


def wikidata_geolocation_to_text(latitude: float, longitude: float):
    """
    Convert a Wikidata geolocation value into natural language text.
    """
    latitude = abs(latitude)
    hemi = 'N' if latitude >= 0 else 'S'

    degrees = int(latitude)
    minutes_full = (latitude - degrees) * 60
    minutes = int(minutes_full)
    seconds = (minutes_full - minutes) * 60

    # Round to-tenth of a second, drop trailing .0
    seconds = round(seconds, 1)
    seconds_str = f"{seconds}".rstrip("0").rstrip(".")

    lat_str = f"{degrees}°{minutes}'{seconds_str}\"{hemi}"

    longitude = abs(longitude)
    hemi = 'E' if longitude >= 0 else 'W'

    degrees = int(longitude)
    minutes_full = (longitude - degrees) * 60
    minutes = int(minutes_full)
    seconds = (minutes_full - minutes) * 60

    # Round to-tenth of a second, drop trailing .0
    seconds = round(seconds, 1)
    seconds_str = f"{seconds}".rstrip("0").rstrip(".")

    lon_str = f"{degrees}°{minutes}'{seconds_str}\"{hemi}"

    return f'{lat_str}, {lon_str}'