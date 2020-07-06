from fastapi import FastAPI
from query_entry import async_search
from typing import Optional
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionError
import uvicorn

timeout = 5

api = FastAPI()
es = AsyncElasticsearch(timeout=timeout)


@api.get("/search")
async def search_endpoint(q: str = "", fuzziness: Optional[bool] = True):
    if len(q) < 3:
        result = None
    else:
        try:
            result = await async_search(q, es, fuzziness)
        except ConnectionError:
            return {
                "query": q or "",
                "found": False,
                "compound": {},
                "reason": f"Time out, max request time is {timeout}s."
            }
    return {
        "query": q or "",
        "found": result is not None,
        "compound": result or {}
    }


@api.on_event("shutdown")
async def shutdown():
    await es.close()


uvicorn.run(api, port=11451)
