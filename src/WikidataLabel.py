from sqlalchemy import Column, String, DateTime, create_engine, text
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import sessionmaker, declarative_base

from datetime import datetime, timedelta
import requests
import os
import json

"""
MySQL database setup for storing Wikidata labels in all languages.
"""

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "label")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))

LABEL_UNLIMITED = os.environ.get("LABEL_UNLIMITED", "false") == "true"
LABEL_TTL_DAYS = int(os.environ.get("LABEL_TTL_DAYS", "90"))
LABEL_MAX_ROWS = int(os.environ.get("LABEL_MAX_ROWS", "10000000"))
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "15"))

DATABASE_URL = (
    f"mariadb+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    f"?charset=utf8mb4"
)

engine = create_engine(
    DATABASE_URL,
    pool_size=5,  # Limit the number of open connections
    max_overflow=10,  # Allow extra connections beyond pool_size
    pool_recycle=1800,  # Recycle connections every 30 minutes
    pool_pre_ping=True,
)

Base = declarative_base()
Session = sessionmaker(bind=engine, expire_on_commit=False)

class WikidataLabel(Base):
    __tablename__ = 'labels'
    id = Column(String(64), primary_key=True)
    labels = Column(JSON, default=dict)
    date_added = Column(DateTime, default=datetime.now, index=True)

    @staticmethod
    def initialize_database():
        """
        Create tables if they don't already exist.
        """
        try:
            Base.metadata.create_all(engine)
            return True
        except Exception as e:
            print(f"Error while initializing labels database: {e}")
            return False

    @staticmethod
    def add_bulk_labels(data):
        """
        Insert multiple label records in bulk.

        Parameters:
        - data (list[dict]): A list of dictionaries, each containing 'id', 'labels' keys.

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        if not data:
            return True

        for i in range(len(data)):
            data[i]['date_added'] = datetime.now()
            if isinstance(data[i].get("labels"), dict):
                data[i]["labels"] = json.dumps(data[i]["labels"], ensure_ascii=False, separators=(",", ":"))


        with Session() as session:
            try:
                session.execute(text('''
                    INSERT INTO labels (id, labels, date_added)
                    VALUES (:id, :labels, :date_added)
                    ON DUPLICATE KEY UPDATE
                    labels = VALUES(labels),
                    date_added = VALUES(date_added)
                '''), data)

                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
                return False

    @staticmethod
    def add_label(id, labels):
        """
        Insert a labels and descriptions into the database.

        Parameters:
        - id (str): The unique identifier for the entity.
        - labels (dict): A dictionary of labels (e.g. { "en": "Label in English", "fr": "Label in French", ... }).

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        with Session() as session:
            try:
                new_entry = WikidataLabel(
                    id=id,
                    labels=labels
                )
                session.add(new_entry)
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
                return False

    @staticmethod
    def get_labels(id):
        """
        Retrieve labels and descriptions for a given entity by its ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - dict: The labels dictionary if found, otherwise an empty dict.
        """
        try:
            with Session() as session:
                # Get labels that are less than LABEL_TTL_DAYS old
                date_limit = (datetime.now() - timedelta(days=LABEL_TTL_DAYS))
                item = session.query(WikidataLabel)\
                    .filter(
                        WikidataLabel.id == id,
                        WikidataLabel.date_added >= date_limit
                    ).first()

                if item is not None:
                    return item.labels or {}
        except Exception as e:
            print(f"Error while fetching cached label {id}: {e}")

        labels = WikidataLabel._get_labels_wdapi(id).get(id)
        if labels:
            WikidataLabel.add_label(id, labels)

        return labels

    @staticmethod
    def get_bulk_labels(ids):
        """
        Retrieve labels for multiple entities by their IDs.

        Parameters:
        - ids (list[str]): A list of entity IDs to retrieve.

        Returns:
        - dict[str, dict]: A dictionary mapping each ID to its labels.
        """
        if not ids:
            return {}

        labels = {}
        try:
            with Session() as session:
                # Get labels that are less than LABEL_TTL_DAYS old
                date_limit = (datetime.now() - timedelta(days=LABEL_TTL_DAYS))
                rows = session.query(WikidataLabel.id, WikidataLabel.labels)\
                    .filter(
                        WikidataLabel.id.in_(ids),
                        WikidataLabel.date_added >= date_limit
                    ).all()
                labels = {id: labels for id, labels in rows}
        except Exception as e:
            print(f"Error while fetching cached labels in bulk: {e}")

        # Fallback when labels are missing from the database
        missing_ids = set(ids) - set(labels.keys())
        if missing_ids:
            missing_labels = WikidataLabel._get_labels_wdapi(missing_ids)
            labels.update(missing_labels)

            # Cache labels
            WikidataLabel.add_bulk_labels([
                {'id': entity_id, 'labels': entity_labels}
                for entity_id, entity_labels in missing_labels.items()
            ])

        return labels

    @staticmethod
    def delete_old_labels():
        """
        Delete labels older than X days.
        If the database exceeds 10 million rows, delete the oldest rows until it is below the threshold.
        """
        if LABEL_UNLIMITED:
            return True

        with Session() as session:
            try:
                # Step 1: Delete labels older than X days
                date_limit = (datetime.now() - timedelta(days=LABEL_TTL_DAYS))
                session.execute(
                    text("DELETE FROM labels WHERE date_added < :date_limit"),
                    {"date_limit": date_limit}
                )
                session.commit()

                # Step 2: Check total count
                total_count = session.execute(text("SELECT COUNT(*) FROM labels")).scalar()

                if total_count > LABEL_MAX_ROWS:
                    # Calculate how many rows to delete
                    rows_to_delete = total_count - LABEL_MAX_ROWS

                    # Delete oldest rows by date_added (MySQL-safe form)
                    session.execute(
                        text("""
                            DELETE l
                            FROM labels AS l
                            JOIN (
                                SELECT id
                                FROM labels
                                ORDER BY date_added ASC
                                LIMIT :rows_to_delete
                            ) AS old_labels ON l.id = old_labels.id
                        """),
                        {"rows_to_delete": rows_to_delete}
                    )

                    session.commit()

                return True
            except Exception as e:
                session.rollback()
                print(f"Error while deleting old labels: {e}")
                return False

    @staticmethod
    def _get_labels_wdapi(ids):
        """
        Retrieve labels from the Wikidata API for a list of IDs.

        Parameters:
        - ids (list[str] or str): A list of Wikidata entity IDs or a single string of IDs separated by '|'.

        Returns:
        - dict: A dictionary mapping each ID to its labels.
        """
        entities_data = {}

        if isinstance(ids, str):
            ids = ids.split('|')
        ids = list(set(ids)) # Ensure unique IDs

        # Wikidata API has a limit on the number of IDs per request, typically 50 for wbgetentities.
        for chunk_idx in range(0, len(ids), 50):

            ids_chunk = ids[chunk_idx:chunk_idx+50]
            ids_chunk = "|".join(ids_chunk)
            params = {
                'action': 'wbgetentities',
                'ids': ids_chunk,
                'props': 'labels',
                'format': 'json',
                'origin': '*',
            }
            headers = {
                'User-Agent': 'Wikidata Textifier (embedding@wikimedia.de)'
            }

            response = requests.get(
                "https://www.wikidata.org/w/api.php?",
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            chunk_data = response.json().get("entities", {})
            entities_data = entities_data | chunk_data

        entities_data = WikidataLabel._compress_labels(entities_data)
        return entities_data

    @staticmethod
    def _compress_labels(data):
        """
        Compress labels by extracting the 'value' field from each label.

        Parameters:
        - data (dict): A dictionary of labels from Wikidata API.

        Returns:
        - dict: A new dictionary with labels compressed to their 'value' field.
        """
        new_labels = {}
        for qid, labels in data.items():
            if 'labels' in labels:
                new_labels[qid] = {
                    lang: label.get('value') \
                        for lang, label in labels['labels'].items()
                }
            else:
                new_labels[qid] = {}
        return new_labels

    @staticmethod
    def get_lang_val(data, lang='en', fallback_lang=None):
        """
        Extracts the value for a given language from a dictionary of labels.
        """
        label = data.get(lang, data.get('mul', {}))
        if fallback_lang and not label:
            label = data.get(fallback_lang, {})

        if isinstance(label, str):
            return label
        return label.get('value', '')

    @staticmethod
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
            if ('claims' in data) and isinstance(data['claims'], dict):
                ids_list = ids_list | data['claims'].keys()

            for _, value in data.items():
                ids_list = ids_list | WikidataLabel.get_all_missing_labels_ids(value)

        elif isinstance(data, list):
            for item in data:
                ids_list = ids_list | WikidataLabel.get_all_missing_labels_ids(item)

        return ids_list

class LazyLabel:
    def __init__(self, qid, factory):
        self.qid = qid
        self.factory = factory

    def __str__(self):
        self.factory.resolve_all()
        return self.factory.get_label(self.qid)

class LazyLabelFactory:
    def __init__(self, lang='en', fallback_lang='en'):
        self.lang = lang
        self.fallback_lang = fallback_lang
        self._pending_ids = set()
        self._resolved_labels = {}

    def create(self, qid: str) -> "LazyLabel":
        self._pending_ids.add(qid)
        return LazyLabel(qid, factory=self)

    def resolve_all(self):
        if not self._pending_ids:
            return

        self._pending_ids = self._pending_ids - set(self._resolved_labels.keys())
        label_data = WikidataLabel.get_bulk_labels(list(self._pending_ids))
        self._resolved_labels.update(label_data)
        self._pending_ids.clear()

    def get_label(self, qid: str) -> str:
        label_dict = self._resolved_labels.get(qid, {})
        label = WikidataLabel.get_lang_val(label_dict, lang=self.lang, fallback_lang=self.fallback_lang)
        return label

    def set_lang(self, lang: str):
        self.lang = lang
        self.resolve_all()
