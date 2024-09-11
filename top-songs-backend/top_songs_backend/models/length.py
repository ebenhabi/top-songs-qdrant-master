
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class Length(BaseModel):
    name: str

    def __repr__(self):
        return (f'<Length(id={self.id}, '
                f'name={self.name})>')
