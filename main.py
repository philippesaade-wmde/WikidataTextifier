from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi import BackgroundTasks
import traceback

from src.Normalizer import TTLNormalizer, JSONNormalizer
from src.WikidataLabel import WikidataLabel
from src import utils

# Start Fastapi app
app = FastAPI(
    title="Wikidata Textifier",
    description="Transforms Wikidata entities into text representations.",
    version="1.0.0",
    docs_url="/docs",  # Change the Swagger UI path if needed
    redoc_url="/redoc",  # Change the ReDoc path if needed
    swagger_ui_parameters={"persistAuthorization": True},
)

# Enable all Cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get(
    "/",
    responses={
        200: {
            "description": "Returns a list of relevant Wikidata property PIDs with similarity scores",
            "content": {
                "application/json": {
                    "example": [{
                        "Q42": "Douglas Adams (human), English writer, humorist, and dramatist...",
                    }]
                }
            },
        },
        422: {
            "description": "Missing or invalid query parameter",
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid format specified"}
                }
            },
        },
    },
)
async def get_textified_wd(
    request: Request, background_tasks: BackgroundTasks,
    id: str = Query(..., examples="Q42,Q2"),
    pid: str = Query(None, examples="P31,P279"),
    lang: str = 'en',
    format: str = 'json',
    external_ids: bool = True,
    references: bool = False,
    all_ranks: bool = False,
    fallback_lang: str = 'en'
):
    """
    Retrieve a Wikidata item with all labels or textual representations for an LLM.

    Args:
        id (str): The Wikidata item ID (e.g., "Q42").
        pid (str): Comma-separated list of property IDs to filter claims (e.g., "P31,P279").
        format (str): The format of the response, either 'json', 'text', or 'triplet'.
        lang (str): The language code for labels (default is 'en').
        external_ids (bool): If True, includes external IDs in the response.
        all_ranks (bool): If True, includes statements of all ranks (preferred, normal, deprecated).
        references (bool): If True, includes references in the response. (only available in JSON format)
        fallback_lang (str): The fallback language code if the preferred language is not available.

    Returns:
        list: A list of dictionaries containing QIDs and the similarity scores.
    """
    try:

        if not id:
            response = "ID is missing"
            return HTTPException(status_code=422, detail=response)

        filter_pids = []
        if pid:
            filter_pids = [p.strip() for p in pid.split(',')]

        qids = [q.strip() for q in id.split(',')]

        entities = {}
        if len(qids) == 1:
            # When one QID is requested, TTL is used
            entity_data = utils.get_wikidata_ttl_by_id(qids[0], lang=lang)
            if not entity_data:
                response = "ID not found"
                return HTTPException(status_code=404, detail=response)

            entity_data = TTLNormalizer(
                entity_id=qids[0],
                ttl_text=entity_data,
                lang=lang,
                fallback_lang=fallback_lang,
                debug=False,
            )

            entities = {
                qids[0]: entity_data.normalize(
                    external_ids=external_ids,
                    all_ranks=all_ranks,
                    references=references,
                    filter_pids=filter_pids
                )
            }
        else:
            # JSON is used with Action API for bulk retrieval
            entity_data = utils.get_wikidata_json_by_ids(qids)
            if not entity_data:
                response = "IDs not found"
                return HTTPException(status_code=404, detail=response)

            entity_data = {
                qid: JSONNormalizer(
                    entity_id=qid,
                    entity_json=entity_data[qid],
                    lang=lang,
                    fallback_lang=fallback_lang,
                    debug=False,
                ) if entity_data.get(qid) else None
                for qid in qids
            }

            entities = {
                qid: entity.normalize(
                    external_ids=external_ids,
                    all_ranks=all_ranks,
                    references=references,
                    filter_pids=filter_pids
                ) if entity else None
                for qid, entity in entity_data.items()
            }

        return_data = {}
        for qid, entity in entities.items():
            if not entity:
                return_data[qid] = None
                continue

            if format == 'text':
                results = str(entity)
            elif format == 'triplet':
                results = entity.to_triplet()
            else:
                results = entity.to_json()

            return_data[qid] = results

        background_tasks.add_task(WikidataLabel.delete_old_labels)
        return return_data

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")
