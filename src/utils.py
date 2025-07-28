from .WikidataLabel import WikidataLabel
import requests
from datetime import datetime, date

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

def time_to_text(time_data, lang='en'):
    """
    Converts Wikidata time data into a human-readable string.

    Parameters:
    - time_data (dict): A dictionary containing the time string, precision, and calendar model.
    - lang (str): The language code for the output (currently not supported).

    Returns:
    - str: A textual representation of the time with appropriate granularity.
    """
    if time_data is None:
        return None

    time_value = time_data['time']
    precision = time_data['precision']
    calendarmodel = time_data.get('calendarmodel', 'http://www.wikidata.org/entity/Q1985786')

    # Use regex to parse the time string
    pattern = r'([+-])(\d{1,16})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'
    match = re.match(pattern, time_value)

    if not match:
        raise ValueError("Malformed time string")

    sign, year_str, month_str, day_str, hour_str, minute_str, second_str = match.groups()
    year = int(year_str) * (1 if sign == '+' else -1)

    # Convert Julian to Gregorian if necessary
    if 'Q1985786' in calendarmodel and year > 1 and len(str(abs(year))) <= 4:  # Julian calendar
        try:
            month = 1 if month_str == '00' else int(month_str)
            day = 1 if day_str == '00' else int(day_str)
            julian_date = date(year, month, day)
            gregorian_ordinal = julian_date.toordinal() + (datetime(1582, 10, 15).toordinal() - datetime(1582, 10, 5).toordinal())
            gregorian_date = date.fromordinal(gregorian_ordinal)
            year, month, day = gregorian_date.year, gregorian_date.month, gregorian_date.day
        except ValueError:
            raise ValueError("Invalid date for Julian calendar")
    else:
        month = int(month_str) if month_str != '00' else 1
        day = int(day_str) if day_str != '00' else 1

    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_str = months[month - 1] if month != 0 else ''
    era = 'AD' if year > 0 else 'BC'

    if precision == 14:
        return f"{year} {month_str} {day} {hour_str}:{minute_str}:{second_str}"
    elif precision == 13:
        return f"{year} {month_str} {day} {hour_str}:{minute_str}"
    elif precision == 12:
        return f"{year} {month_str} {day} {hour_str}:00"
    elif precision == 11:
        return f"{day} {month_str} {year}"
    elif precision == 10:
        return f"{month_str} {year}"
    elif precision == 9:
        return f"{abs(year)} {era}"
    elif precision == 8:
        decade = (year // 10) * 10
        return f"{abs(decade)}s {era}"
    elif precision == 7:
        century = (abs(year) - 1) // 100 + 1
        return f"{century}th century {era}"
    elif precision == 6:
        millennium = (abs(year) - 1) // 1000 + 1
        return f"{millennium}th millennium {era}"
    elif precision == 5:
        tens_of_thousands = abs(year) // 10000
        return f"{tens_of_thousands} ten thousand years {era}"
    elif precision == 4:
        hundreds_of_thousands = abs(year) // 100000
        return f"{hundreds_of_thousands} hundred thousand years {era}"
    elif precision == 3:
        millions = abs(year) // 1000000
        return f"{millions} million years {era}"
    elif precision == 2:
        tens_of_millions = abs(year) // 10000000
        return f"{tens_of_millions} tens of millions of years {era}"
    elif precision == 1:
        hundreds_of_millions = abs(year) // 100000000
        return f"{hundreds_of_millions} hundred million years {era}"
    elif precision == 0:
        billions = abs(year) // 1000000000
        return f"{billions} billion years {era}"
    else:
        raise ValueError(f"Unknown precision value {precision}")


def quantity_to_text(quantity_data, labels={}, lang='en'):
    """
    Converts Wikidata quantity data into a human-readable string.

    Parameters:
    - quantity_data (dict): A dictionary with 'amount' and optionally 'unit' (often a QID).
    - labels (dict): A dictionary mapping QIDs to their labels, previously fetched.
    - lang (str): The language code for the output.

    Returns:
    - str: A textual representation of the quantity (e.g., "5 kg").
    """
    if quantity_data is None:
        return None

    quantity = quantity_data.get('amount')
    unit = quantity_data.get('unit')

    # 'unit' of '1' means that the value is a count and doesn't require a unit.
    if unit == '1':
        unit = None
    else:
        unit_qid = unit.rsplit('/')[-1]
        if unit_qid in labels:
            unit = labels[unit_qid]
        else:
            unit = WikidataLabel.get_labels(unit_qid)
        unit = get_lang_val(unit, lang=lang)

    return quantity + (f" {unit}" if unit else "")


def globalcoordinate_to_text(coor_data, lang='en'):
    """
    Convert a single decimal degree value to DMS with hemisphere suffix.
    `hemi_pair` is ("N", "S") for latitude or ("E", "W") for longitude.

    Parameters:
    - coor_data (dict): A dictionary with 'latitude' and 'longitude' keys.
    - lang (str): The language code for the output (currently not supported).

    Returns:
    - str: A string representation of the coordinates in DMS format.
    """

    latitude = abs(coor_data['latitude'])
    hemi = 'N' if coor_data['latitude'] >= 0 else 'S'

    degrees = int(latitude)
    minutes_full = (latitude - degrees) * 60
    minutes = int(minutes_full)
    seconds = (minutes_full - minutes) * 60

    # Round to-tenth of a second, drop trailing .0
    seconds = round(seconds, 1)
    seconds_str = f"{seconds}".rstrip("0").rstrip(".")

    lat_str = f"{degrees}°{minutes}'{seconds_str}\"{hemi}"

    longitude = abs(coor_data['longitude'])
    hemi = 'E' if coor_data['longitude'] >= 0 else 'W'

    degrees = int(longitude)
    minutes_full = (longitude - degrees) * 60
    minutes = int(minutes_full)
    seconds = (minutes_full - minutes) * 60

    # Round to-tenth of a second, drop trailing .0
    seconds = round(seconds, 1)
    seconds_str = f"{seconds}".rstrip("0").rstrip(".")

    lon_str = f"{degrees}°{minutes}'{seconds_str}\"{hemi}"

    return f'{lat_str}, {lon_str}'

def get_lang_val(data, lang='en'):
    """
    Extracts the value for a given language from a dictionary of labels.
    """
    label = data.get(lang, data.get('mul', {}))
    if isinstance(label, str):
        return label
    return label.get('value', '')