
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class Format(BaseModel):
    name: str

    def __repr__(self):
        return (f'<Format(id={self.id}, '
                f'name={self.name})>')
