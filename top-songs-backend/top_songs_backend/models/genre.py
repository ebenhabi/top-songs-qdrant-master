
from dataclasses import dataclass

from pydantic import BaseModel
from typing import List


@dataclass
class Genre(BaseModel):
    name: str

    def __repr__(self):
        return (f'<Genre(id={self.id}, '
                f'name={self.name})>')


class Genres(BaseModel):
    genre: List[Genre]
