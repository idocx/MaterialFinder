import re
from elasticsearch import Elasticsearch
from functools import reduce
from typing import Dict, Union

es = Elasticsearch()
db_name = "compounds"


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

    def is_full_matched(self, word_threshold=2, char_threshold=10) -> bool:
        """
        A simple post-filter
        """
        for highlight in self._highlights:
            unmatched = re.sub(self.match_pattern, " ", highlight)
            unmatched_words = re.findall("[A-z]+", unmatched)
            unmatched_char_number = sum(map(len, unmatched_words))
            if len(unmatched_words) <= word_threshold and \
                    unmatched_char_number <= char_threshold:
                self.source["hit"] = re.sub(self.remove_tag_pattern, "", highlight)
                return True
        return False

    @classmethod
    def parse_hit(cls, raw_hit: dict):
        source = raw_hit["_source"]
        # temporarily disable synonyms for short response
        source.pop("synonyms", None)
        score = raw_hit.get("_score", None)
        highlights = raw_hit.get("highlight", {}).values()
        highlights = reduce(lambda hs, h: hs.extend(h[:3]) or hs, highlights)
        return cls(source, highlights, score)


def parse_hits(results):
    yield from (Hit.parse_hit(result) for result in results)


def search(query: str) -> Union[Dict, None]:
    query = query.lower()
    words = " ".join(re.findall(r"[a-z]+", query))
    numbers = " ".join(re.findall(r"\d+", query))
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
                                "synonyms.synonym": {
                                  "query": words,
                                  "operator": "and"
                                }
                              }
                            }
                          ],
                          "should": [
                            {
                              "match": {
                                "synonyms.synonym": {
                                  "query": numbers,
                                  "boost": 0.4
                                }
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
                            "title": {
                              "query": words,
                              "operator": "and"
                            }
                          }
                        }
                      ],
                      "should": [
                        {
                          "match": {
                            "title": {
                              "query": numbers,
                              "boost": 0.4
                            }
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
                    "title": {
                      "query": words,
                      "slop": 3
                    }
                  }
                },
                {
                  "nested": {
                    "path": "synonyms",
                    "query": {
                      "match_phrase": {
                        "synonyms.synonym": {
                          "query": words,
                          "slop": 3
                        }
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
    results = es.search(body=query_body, index=db_name)["hits"]["hits"]
    hits = parse_hits(results)
    for hit in hits:
        if hit.is_full_matched():
            return hit.source
    return None


if __name__ == '__main__':
    print(search("chlorodifluoromethane"))
