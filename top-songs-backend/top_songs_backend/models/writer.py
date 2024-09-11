
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class Writer(BaseModel):
    name: str

    def __repr__(self):
        return (f'<Writer(id={self.id}, '
                f'name={self.name})>')
