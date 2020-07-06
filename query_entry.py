import re
from utils import common_groups
from functools import reduce
from typing import Dict, Optional
from elasticsearch import Elasticsearch, AsyncElasticsearch


class Hit:
    default_pre_tag = "<em>"
    default_post_tag = "</em>"
    match_pattern = re.compile(rf"({default_pre_tag}.+?{default_post_tag})")
    remove_tag_pattern = re.compile(rf"({default_pre_tag}|{default_post_tag})")

    def __init__(self, source, highlights=None, score=None):
        self.source = source
        self._highlights = highlights
        self._score = score

    @property
    def highlights(self):
        return self._highlights

    def is_full_matched(self, word_threshold=2, char_threshold=0.3) -> bool:
        """
        A simple post-filter
        """
        for highlight in self._highlights:
            # the total number of chars
            origin_name = re.sub(self.remove_tag_pattern, "", highlight)
            char_number = sum(map(len, re.findall(r"\w+", origin_name)))

            # remove highlighted words
            unmatched = re.sub(self.match_pattern, " ", highlight)

            # count all the unmatched words
            unmatched_words = re.findall("[A-z]+", unmatched)
            # filter common chemical groups
            contain_extra_words = any(unmatched_word in common_groups for unmatched_word in unmatched_words)
            unmatched_char_number = sum(map(len, unmatched_words))
            if len(unmatched_words) <= word_threshold and \
                    unmatched_char_number / char_number <= char_threshold and \
                    not contain_extra_words:
                self.source["hit"] = origin_name
                return True
        return False

    @classmethod
    def parse_hit(cls, raw_hit: dict):
        source = raw_hit["_source"]
        # temporarily disable synonyms for short response
        source.pop("synonyms", None)
        score = raw_hit.get("_score", None)
        highlights = raw_hit.get("highlight", {}).values()
        highlights = reduce(lambda hs, h: hs.extend(h[:5]) or hs, highlights, [])
        return cls(source, highlights, score)


def generate_query_body(query: str, allow_fuzziness: bool) -> Dict:
    """
    get the query dict used for elasticsearch

    fuzziness policy:
        if allow_fuzziness is True and length of words in query > 6:
            enable fuzziness
        else:
            disable fuzziness
    """
    # lower the query and split words and numbers
    query: str = query.lower()
    words: str = " ".join(re.findall(r"[a-z]+", query))
    numbers: str = " ".join(re.findall(r"\d+", query))

    # search query for matching words
    query_words = {
        "query": words,
        "operator": "and",
    }

    # search query for matching numbers
    query_numbers = {
        "query": numbers,
        "boost": 0.4
    }

    # search query for matching phrase
    query_phrase = {
        "query": words,
        "slop": 3
    }

    # enable / disable fuzziness search
    if allow_fuzziness:
        if len(words) > 14:
            query_words.update(fuzziness=2)
        elif len(words) > 6:
            query_words.update(fuzziness=1)

    # query to be sent
    query_body = {
      "size": 3,
      "query": {
        "bool": {
          "must": [
            {
              "dis_max": {
                "tie_breaker": 0,
                "boost": 1,
                "queries": [
                  {
                    "nested": {
                      "path": "synonyms",
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "synonyms.synonym": query_words
                              }
                            }
                          ],
                          "should": [
                            {
                              "match": {
                                "synonyms.synonym": query_numbers
                              }
                            }
                          ]
                        }
                      },
                      "score_mode": "max"
                    }
                  },
                  {
                    "bool": {
                      "must": [
                        {
                          "match": {
                            "title": query_words
                          }
                        }
                      ],
                      "should": [
                        {
                          "match": {
                            "title": query_numbers
                          }
                        }
                      ],
                      "boost": 1.1
                    }
                  }
                ]
              }
            }
          ],
          "should": [
            {
              "rank_feature": {
                "field": "uid",
                "boost": "1.1"
              }
            }
          ]
        }
      },
      "rescore": {
        "query": {
          "rescore_query": {
            "dis_max": {
              "tie_breaker": 0.7,
              "boost": 1.2,
              "queries": [
                {
                  "match_phrase": {
                    "title": query_phrase
                  }
                },
                {
                  "nested": {
                    "path": "synonyms",
                    "query": {
                      "match_phrase": {
                        "synonyms.synonym": query_phrase
                      }
                    },
                    "score_mode": "max"
                  }
                }
              ]
            }
          },
          "query_weight": 1.2,
          "rescore_query_weight": 0.8
        },
        "window_size": 10
      },
      "highlight": {
        "number_of_fragments": 0,
        "type": "unified",
        "order": "score",
        "fields": {
          "title": {},
          "synonyms.synonym": {}
        }
      }
    }

    return query_body


def parse_response(results) -> Optional[Dict]:
    """
    parse the json file that elasticsearch returns
    """
    results: Dict = results["hits"]["hits"]
    hits = (Hit.parse_hit(result) for result in results)
    for hit in hits:
        if hit.is_full_matched():
            return hit.source
    return None  # None means no matches


def search(query: str, es: Elasticsearch,
           allow_fuzziness: bool, index="compounds") \
        -> Optional[Dict]:
    """
    sync version of search function
    """
    query_body = generate_query_body(query, allow_fuzziness=allow_fuzziness)
    results = es.search(body=query_body, index=index)
    return parse_response(results)


async def async_search(query: str, es: AsyncElasticsearch,
                       allow_fuzziness: bool, index="compounds") \
        -> Optional[Dict]:
    """
    async version of search function
    """
    query_body = generate_query_body(query, allow_fuzziness=allow_fuzziness)
    results = await es.search(body=query_body, index=index)
    return parse_response(results)


if __name__ == '__main__':
    from pymongo import MongoClient
    from tqdm import tqdm
    import asyncio

    elastic = AsyncElasticsearch()

    db = MongoClient()["materials"]
    collection = db["material_entities"]


    async def generate():
        for material in tqdm(collection.find()):
            yield material

    async def main():
        async for material in generate():
            await async_search(material["material_string"], elastic, True)

    asyncio.run(main())
