
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class Producer(BaseModel):
    name: str

    def __repr__(self):
        return (f'<Producer(id={self.id}, '
                f'name={self.name})>')
