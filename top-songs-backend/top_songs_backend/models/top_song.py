# Top Song Model

from dataclasses import dataclass
from typing import List, Optional, Union
from pydantic import BaseModel, Field

from qdrant_client.models import RecommendStrategy

from .album import Album
from .artist import Artist
from .format import Format
from .genre import Genres, Genre
from .length import Length
from .weeks import Weeks, WeeksFilter
from .writer import Writer
from .producer import Producer

from top_songs_backend.config import config

TopSongId: Union[int, str]


class SearchQuery(BaseModel):
    week: Optional[WeeksFilter] = None
    positive: Optional[List[TopSongId]] = None
    negative: Optional[List[TopSongId]] = None
    queries: Optional[List[str]] = None
    limit: int = Field(config.DEFAULT_LIMIT, ge=1, le=config.MAX_SEARCH_LIMIT)
    strategy: RecommendStrategy = RecommendStrategy.BEST_SCORE


@dataclass
class TopSong(BaseModel):
    id: int
    title: str
    href: str
    label: str
    released: str
    recorded: str
    description: str
    artist: Artist
    weeks: Weeks
    album: Album
    formats: List[Format]
    genres: Genres
    lengths: List[Length]
    writers: List[Writer]
    producers: List[Producer]
    payload: dict
    score: int

    def __repr__(self):
        return (f'<TopSong(if={self.id}, '
                f'title={self.title}, '
                f'href={self.href}, '
                f'label={self.label}, '
                f'released={self.released}, '
                f'recorded={self.recorded}, '
                f'description={self.description})>')

    @classmethod
    def from_point(cls, point):
        artist = Artist(
            name=point.payload['name'],
            href=point.payload['href']
        )

        genres = Genres(
            genre=point.payload['genres']['genre']
        )

        weeks = Weeks(
            last=point.payload['weeks']['last'],
            week=point.payload['weeks']['week']
        )

        return TopSong(
            id=point.id,
            title=point.payload['title'],
            href=point.payload['href'],
            label=point.payload['label'],
            released=point.payload['released'],
            recorded=point.payload['recorded'],
            description=point.payload['description'],
            artist=artist,
            weeks=weeks,
            album=point.payload['album'],
            formats=point.payload['formats'],
            genres=genres,
            lengths=point.payload['lengths'],
            writers=point.payload['writers'],
            producers=point.payload['producers'],
            payload=point.payload,
            score=point.score
        )
