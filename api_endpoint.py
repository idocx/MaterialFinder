from fastapi import FastAPI
from query_entry import search

api = FastAPI()


@api.get("/")
def index_guide():
    return {
        "search_endpoint": {
            "url": "/api/search",
            "params": {
                "query": "string"
            }
        }
    }


@api.get("/api/search")
def search_endpoint(query: str):
    result = search(query)
    return {
        "found": result is not None,
        "compound": result or {}
    }
