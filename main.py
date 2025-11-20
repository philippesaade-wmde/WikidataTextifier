from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi import BackgroundTasks
import traceback

from src.WikidataTextifier import WikidataEntity
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
    all_ranks: bool = False
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

    Returns:
        list: A list of dictionaries containing QIDs and the similarity scores.
    """
    try:

        if not id:
            response = "ID is missing"
            return HTTPException(status_code=422, detail=response)

        filter_pids = None
        if pid:
            filter_pids = [p.strip() for p in pid.split(',')]

        qids = [q.strip() for q in id.split(',')]
        entity_dict = utils.get_wikidata_entities_by_ids(qids)

        if not entity_dict:
            response = "ID not found"
            return HTTPException(status_code=404, detail=response)

        return_data = {}
        for id in qids:
            if id in entity_dict:
                entity = WikidataEntity.from_wd(
                    entity_dict[id],
                    id=id,
                    lang=lang,
                    external_ids=external_ids,
                    all_ranks=all_ranks,
                    references=references,
                    filter_pids=filter_pids
                )

                if format == 'text':
                    results = str(entity)
                elif format == 'triplet':
                    results = entity.to_triplet()
                else:
                    results = entity.to_json()

                return_data[id] = results
            else:
                return_data[id] = None

        if len(qids) == 1:
            return_data = return_data[qids[0]]
            if not return_data:
                response = "Item not found"
                return HTTPException(status_code=404, detail=response)

        background_tasks.add_task(WikidataLabel.delete_old_labels)
        return return_data

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")
