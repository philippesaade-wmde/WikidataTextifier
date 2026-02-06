import requests
import json
import html
import os

def get_wikidata_ttl_by_id(
        id,
        lang='en',
    ):
    """Fetches a Wikidata entity by its ID and returns its TTL representation.

    Args:
        id (str): A Wikidata entity ID (e.g., Q42, P31).
        lang (str, optional): The language to use for the response. Defaults to 'en'.

    Returns:
        str: The TTL representation of the entity.
    """
    params = {
        'uselang': lang,
    }
    headers = {
        'User-Agent': 'Wikidata Textifier (embeddings@wikimedia.de)'
    }

    response = requests.get(
        f"https://www.wikidata.org/wiki/Special:EntityData/{id}.ttl",
        params=params,
        headers=headers
    )
    response.raise_for_status()
    return response.text


def get_wikidata_json_by_ids(
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
            'User-Agent': 'Wikidata Textifier (embeddings@wikimedia.de)'
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


#####################################
# Formatting
#####################################

def wikidata_time_to_text(value: dict, lang: str = "en"):
    """
    Convert a Wikidata time value into natural language text.
    """
    WIKIBASE_HOST = os.environ.get("WIKIBASE_HOST", "wikibase")
    WIKIBASE_API = f"http://{WIKIBASE_HOST}/w/api.php"

    time = value.get("time")
    if time.endswith("+00:00"):
        time = time[:-6] + "Z"
    if not time.startswith("+") and not time.startswith("-"):
        time = "+" + time

    datavalue = {
        "type": "time",
        "value": {
            "time": time,
            "timezone": value.get("timezone", 0),
            "before": value.get("before", 0),
            "after": value.get("after", 0),
            "precision": value.get("precision", 10),
            "calendarmodel": value.get("calendarmodel", "Q1985786"),
        },
    }

    r = requests.post(WIKIBASE_API, data={
        "action": "wbformatvalue",
        "format": "json",
        "uselang": lang,
        "datavalue": json.dumps(datavalue),
    })
    r.raise_for_status()

    data = r.json()
    return html.unescape(data["result"])


def wikidata_geolocation_to_text(value: dict, lang: str = "en"):
    """
    Convert a Wikidata geolocation value into natural language text.
    """
    WIKIBASE_HOST = os.environ.get("WIKIBASE_HOST", "wikibase")
    WIKIBASE_API = f"http://{WIKIBASE_HOST}/w/api.php"

    datavalue = {
        "type": "globecoordinate",
        "value": {
            "latitude": value.get("latitude"),
            "longitude": value.get("longitude"),
            "altitude": value.get("altitude", None),
            "precision": value.get("precision", 0),
            "globe": value.get("globe", "http://www.wikidata.org/entity/Q2"),
        },
    }

    r = requests.post(WIKIBASE_API, data={
        "action": "wbformatvalue",
        "format": "json",
        "uselang": lang,
        "datavalue": json.dumps(datavalue),
    })
    r.raise_for_status()

    data = r.json()
    return html.unescape(data["result"])