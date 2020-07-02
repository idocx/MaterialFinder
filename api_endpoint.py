from fastapi import FastAPI
from query_entry import search
from elasticsearch import Elasticsearch

api = FastAPI()
es = Elasticsearch()


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
    result = search(query, es)
    return {
        "found": result is not None,
        "compound": result or {}
    }
