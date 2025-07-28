import requests

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

def get_lang_val(data, lang='en'):
    """
    Extracts the value for a given language from a dictionary of labels.
    """
    label = data.get(lang, data.get('mul', {}))
    if isinstance(label, str):
        return label
    return label.get('value', '')