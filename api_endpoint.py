from fastapi import FastAPI
from query_entry import search
from typing import Optional
from elasticsearch import Elasticsearch

api = FastAPI()
es = Elasticsearch()


@api.get("/")
def search_endpoint(q: Optional[str] = None):
    if not q:
        result = None
    else:
        result = search(q, es)
    return {
        "query": q or "",
        "found": result is not None,
        "compound": result or {}
    }
