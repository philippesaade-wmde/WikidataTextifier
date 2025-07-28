from typing import Annotated
import time
import os
import traceback

# Import necessary types and classes from FastAPI and other libraries.
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

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
                    "example": {"detail": "ID is missing"}
                }
            },
        },
    },
)
async def property_query_route(
    request: Request,
    id: str = Query(..., example="Q42"),
    lang: str = 'en',
    json: bool = True,
):
    """
    Retrieve a Wikidata item with all labels or textual representations for an LLM.

    Args:
        id (str): The Wikidata item ID (e.g., "Q42").
        json (bool): If True, returns the item in JSON format. Defaults to True.

    Returns:
        list: A list of dictionaries containing QIDs and the similarity scores.
    """
    if not id:
        response = "ID is missing"
        raise HTTPException(status_code=422, detail=response)

    try:
        entity = WikidataEntity.from_id(id, lang=lang)

        if json:
            results = entity.to_json()
        else:
            results = str(entity)

        return results
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")
