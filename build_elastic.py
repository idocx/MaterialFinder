from analyzer_pattern import pattern
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from pymongo import MongoClient
from tqdm import tqdm
import re

es = Elasticsearch()

db_name = "compounds"

analyzer = {
    "char_filter": {
        "sign_filter": {
            "type": "pattern_replace",
            "pattern": r"\W+",
            "replacement": " "
        }
    },
    "tokenizer": {
        "chem_tokenizer": {
            "type": "pattern",
            "pattern": pattern,
            "flags": "CASE_INSENSITIVE"
        }
    },
    "analyzer": {
        "chem_analyzer": {
            "type": "custom",
            "tokenizer": "chem_tokenizer",
            "char_filter": [
                "sign_filter"
            ],
            "filter": [
                "lowercase",
                "trim"
            ]
        }
    }
}

similarity = {
    "modified_tdidf": {
        "type": "scripted",
        "weight_script": {
            "source": "double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0;"
                      "return query.boost * idf;"
        },
        "script": {
            "source": "double norm = 1 / Math.sqrt(doc.length);"
                      "return weight * norm;"
        }
    }
}

mapping = {
    "properties": {
        "title": {
            "type": "text",
            "analyzer": "chem_analyzer",
            "similarity": "modified_tdidf"
        },
        "synonyms": {
            "type": "nested",
            "dynamic": "false",
            "properties": {
                "synonym": {
                    "type": "text",
                    "analyzer": "chem_analyzer",
                    "similarity": "modified_tdidf"
                }
            },
        },
        "smiles": {
            "type": "keyword",
        },
        "formula": {
            "type": "keyword",
        },
        "uid": {
            "type": "rank_feature",
            "positive_score_impact": "false"
        },
        "data_source": {
            "type": "keyword",
        }
    }
}

setting = {
    "settings": {
        "analysis": analyzer,
        "similarity": similarity
    },
    "mappings": mapping
}

# create index
print(es.indices.create(index=db_name, ignore=400, body=setting, include_type_name=False))


db = MongoClient()["materials"]
collection = db["compounds"]


def gen_req():
    new_compounds_filter = {
        # "_sync": False
    }
    for compound in collection.find(new_compounds_filter):
        title = compound.get("title", "")
        synonyms = compound.get("synonyms", [])
        smiles = compound.get("smiles", "")
        formula = compound.get("formula", "")
        uid = compound["uid"]
        data_source = re.search(r"(?<=_).+(?=__)", uid).group()
        uid = int(re.search(r"\d+", uid).group())
        _doc = {
            "uid": uid,
            "data_source": data_source
        }
        if title:
            _doc["title"] = title
        if synonyms:
            synonyms = [{"synonym": synonym} for synonym in synonyms]
            _doc["synonyms"] = synonyms
        if smiles:
            _doc["smiles"] = smiles
        if formula:
            _doc["formula"] = formula
        yield {
            "_index": db_name,
            "_op_type": "index",
            "_id": str(uid),
            "_source": _doc
        }

    print("Finished\nUpdate Mongo...", end="")

    collection.update_many(new_compounds_filter, {"$set": {"_sync": True}})

    print("Done")


for success, info in tqdm(
        parallel_bulk(es, gen_req(), thread_count=8),
        desc="Writing to Elastic ",
        unit="piece"
):
    if not success:
        print('A document failed:', info)
