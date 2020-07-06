from fastapi import FastAPI
from query_entry import async_search
from typing import Optional
from elasticsearch import AsyncElasticsearch
import uvicorn

api = FastAPI()
es = AsyncElasticsearch()


@api.get("/")
async def search_endpoint(q: Optional[str] = None):
    if not q:
        result = None
    else:
        result = await async_search(q, es, True)
    return {
        "query": q or "",
        "found": result is not None,
        "compound": result or {}
    }


@api.on_event("shutdown")
async def shutdown():
    await es.close()


uvicorn.run(api)
