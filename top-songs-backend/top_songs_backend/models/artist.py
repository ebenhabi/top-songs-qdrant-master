
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class Artist(BaseModel):
    name: str
    href: str

    def __repr__(self):
        return (f'<Artist(name={self.name}, '
                f'href={self.href})>')
