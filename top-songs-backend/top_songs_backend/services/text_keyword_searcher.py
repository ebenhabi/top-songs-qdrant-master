import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from qdrant_client import QdrantClient, models

from top_songs_backend.config.config import settings

router = APIRouter()


class TextSearcher:

    def __init__(self):
        self.collection_name = settings.collection_name
        self.text_field_name = settings.text_field_name
        self.model_card = settings.model_card
        self.model_card_sparce = settings.model_card_sparce

        self.url = settings.url
        self.apy_key = settings.api_key
        self.client = QdrantClient(
            url=self.url,
            api_key=self.apy_key
        )
        self.client.set_model(self.model_card)
        self.client.set_sparse_model(self.model_card_sparce)

    def search_text(self, query: str, top_k: int = 5):

        hits = self.client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key=self.text_field_name,
                        match=models.MatchText(text=query)
                    )
                ],
                with_payload=True,
                with_vectors=True,
                limit=top_k
            )
        )

        search_result = [hit.payload for hit in hits[0]]

        return search_result

    def search_query(self, query_text: str, filter_: dict = None, top_k: int = 5):

        hists = self.client.query(
            collection_name=self.collection_name,
            query_text=query_text,
            query_filter=models.Filter(**filter_) if filter_ else None,
            limit=top_k
        )

        search_result = [hit.metadata for hit in hists]
        return search_result

    def search_keyword(self, key: str, keyword: str, top_k: int = 10):
        hits = self.client.recommend(
            collection_name=self.collection_name,
            strategy=models.RecommendStrategy.AVERAGE_VECTOR,
            query_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key=key,
                        match=models.MatchValue(
                            value=keyword
                        )
                    )
                ]
            ),
            limit=top_k
        )

        count = self.client.count(
            collection_name=self.collection_name,
            count_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key=key,
                        match=models.MatchValue(
                            value=keyword
                        ),
                        values_count=models.ValuesCount(

                        )
                    )
                ]
            ),
            exact=True
        )

        print("search_keyword: ", hits)

        search_result = [hit.payload for hit in hits]
        return search_result


@router.get("/search/song")
async def get_songs(q: str):
    text_searcher = TextSearcher()

    query = "Fallin' became Keys"

    return {
        "result": text_searcher.search_query(query_text=query)
    }


if __name__ == '__main__':
    text_searcher = TextSearcher()

    query = "Fallin' became Keys"

    # result = text_searcher.search_query(query_text=query)

    key = "genre"
    keyword = "R&B"

    result = text_searcher.search_keyword(key=key, keyword=keyword, top_k=3)
    print(result)
    print(result[0]['album'])
    print(result[0]['genres'])
    print(result[0]['weeks'])
    print(result[0]['writers'])
