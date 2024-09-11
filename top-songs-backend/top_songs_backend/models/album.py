
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class Album(BaseModel):
    uri: str
    url: str
    name: str

    def __repr__(self) -> str:
        return (f'<Album(id={self.id}, '
                f'uri={self.uri}, '
                f'url={self.url}, '
                f'name={self.name})>')
