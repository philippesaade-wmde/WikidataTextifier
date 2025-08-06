from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
import traceback

from src.WikidataTextifier import WikidataEntity

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
async def property_query_route(
    request: Request,
    id: str = Query(..., examples="Q42"),
    lang: str = 'en',
    format: str = 'json',
    external_ids: bool = True
):
    """
    Retrieve a Wikidata item with all labels or textual representations for an LLM.

    Args:
        id (str): The Wikidata item ID (e.g., "Q42").
        format (str): The format of the response, either 'json', 'text', or 'triplet'.
        lang (str): The language code for labels (default is 'en').
        external_ids (bool): If True, includes external IDs in the response.

    Returns:
        list: A list of dictionaries containing QIDs and the similarity scores.
    """
    if not id:
        response = "ID is missing"
        return HTTPException(status_code=422, detail=response)

    try:
        entity = WikidataEntity.from_id(
            id,
            lang=lang,
            external_ids=external_ids
        )

        if not entity:
            response = "Item not found"
            return HTTPException(status_code=404, detail=response)

        if format == 'json':
            results = entity.to_json()
        elif format == 'triplet':
            results = entity.to_triplet()
        elif format == 'text':
            results = str(entity)
        else:
            response = "Invalid format specified"
            return HTTPException(status_code=422, detail=response)

        return results
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")
