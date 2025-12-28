from sqlalchemy import Column, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator

from datetime import datetime, timedelta
import json
import requests
import os

"""
SQLite database setup for storing Wikidata labels in all languages.
"""
TOOL_DATA_DIR = os.environ.get("TOOL_DATA_DIR", "./data")
DATABASE_URL = os.path.join(TOOL_DATA_DIR, 'sqlite_wikidata_labels.db')

engine = create_engine(f'sqlite:///{DATABASE_URL}',
    pool_size=5,  # Limit the number of open connections
    max_overflow=10,  # Allow extra connections beyond pool_size
    pool_recycle=10  # Recycle connections every 10 seconds
)

Base = declarative_base()
Session = sessionmaker(bind=engine)

class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text
    cache_ok = False

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None

class WikidataLabel(Base):
    __tablename__ = 'labels'
    id = Column(Text, primary_key=True)
    labels = Column(JSONType)
    date_added = Column(Text, default=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    @staticmethod
    def add_bulk_labels(data):
        """
        Insert multiple label records in bulk. If a record with the same ID exists, it is ignored (no update is performed).

        Parameters:
        - data (list[dict]): A list of dictionaries, each containing 'id', 'labels' keys.

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        for i in range(len(data)):
            # Ensure labels are JSON-encoded strings
            if isinstance(data[i]['labels'], dict):
                data[i]['labels'] = json.dumps(
                    data[i]['labels'],
                    separators=(',', ':')
                )

            # Add date_added
            data[i]['date_added'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with Session() as session:
            try:
                session.execute(text('''
                    INSERT INTO labels (id, labels, date_added)
                    VALUES (:id, :labels, :date_added)
                    ON CONFLICT(id) DO UPDATE
                    SET labels = EXCLUDED.labels,
                    date_added = EXCLUDED.date_added
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
        with Session() as session:
            # Get labels that are less than 90 days old
            date_limit = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
            item = session.query(WikidataLabel)\
                .filter(
                    WikidataLabel.id == id,
                    WikidataLabel.date_added >= date_limit
                ).first()

            if item is not None:
                return item.labels

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

        with Session() as session:
            # Get labels that are less than 90 days old
            date_limit = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
            rows = session.query(WikidataLabel.id, WikidataLabel.labels)\
                .filter(
                    WikidataLabel.id.in_(ids),
                    WikidataLabel.date_added >= date_limit
                ).all()
            labels = {id: labels for id, labels in rows}

        # Fallback when labels are missing from the database
        missing_ids = set(ids) - set(labels.keys())
        if missing_ids:
            missing_labels = WikidataLabel._get_labels_wdapi(missing_ids)
            labels.update(missing_labels)

            # Cache labels
            WikidataLabel.add_bulk_labels([
                {'id': id, 'labels': missing_labels[id]} for id in missing_ids
            ])

        return labels

    @staticmethod
    def delete_old_labels():
        """
        Delete labels older than 90 days.
        If the database exceeds 10 million rows, delete the oldest rows until it is below the threshold.
        """
        with Session() as session:
            try:
                # Step 1: Delete labels older than 90 days
                date_limit = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
                session.execute(
                    text("DELETE FROM labels WHERE date_added < :date_limit"),
                    {"date_limit": date_limit}
                )
                session.commit()

                # Step 2: Check total count
                total_count = session.execute(text("SELECT COUNT(*) FROM labels")).scalar()
                max_rows = 10_000_000

                if total_count > max_rows:
                    # Calculate how many rows to delete
                    rows_to_delete = total_count - max_rows

                    # Delete oldest rows by date_added
                    session.execute(text(f"""
                        DELETE FROM labels
                        WHERE id IN (
                            SELECT id FROM labels
                            ORDER BY date_added ASC
                            LIMIT :rows_to_delete
                        )
                    """), {"rows_to_delete": rows_to_delete})

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
                headers=headers
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

class LazyLabel:
    def __init__(self, qid, factory):
        self.qid = qid
        self.factory = factory

    def __str__(self):
        self.factory.resolve_all()
        return self.factory.get_label(self.qid)

class LazyLabelFactory:
    def __init__(self, lang='en'):
        self.lang = lang
        self._pending_ids = set()
        self._resolved_labels = {}

    def create(self, qid: str) -> "LazyLabel":
        self._pending_ids.add(qid)
        return LazyLabel(qid, factory=self)

    def resolve_all(self):
        if not self._pending_ids:
            return
        label_data = WikidataLabel.get_bulk_labels(list(self._pending_ids))
        self._resolved_labels.update(label_data)
        self._pending_ids.clear()

    def get_label(self, qid: str) -> str:
        label_dict = self._resolved_labels.get(qid, {})
        label = WikidataLabel.get_lang_val(label_dict, lang=self.lang, fallback_lang='en')
        return label

    def set_lang(self, lang: str):
        self.lang = lang
        self.resolve_all()


# Create tables if they don't already exist.
Base.metadata.create_all(engine)