from datetime import date, datetime
from dataclasses import dataclass
from typing import List

from pydantic import BaseModel


@dataclass
class Week(BaseModel):
    date: date

    def __repr__(self):
        return (f'<Week(id={self.id}, '
                f'week={self.week})>')


class Weeks(BaseModel):
    last: date
    week: List[Week]

    def __repr__(self):
        return (f'<Weeks(last={self.last}, '
                f'week={self.week})>')


class WeeksFilter(Week):

    def __init__(self, week: date):
        super().__init__()

        self.week = week
        self.decade = self.query_aggregate_by_decade(week=self.week)

    @classmethod
    def query_aggregate_by_decade(cls, week: date) -> str:
        if week > datetime.strptime("2020-01-01", "'%Y-%m-%d'"):
            return "2020s"
        elif datetime.strptime("2010-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("2020-01-01", "'%Y-%m-%d'"):
            return "2010s"
        elif datetime.strptime("2000-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("2010-01-01", "'%Y-%m-%d'"):
            return "2000s"
        elif datetime.strptime("1990-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("2000-01-01", "'%Y-%m-%d'"):
            return "2000s"
        elif datetime.strptime("1980-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("1990-01-01", "'%Y-%m-%d'"):
            return "1990s"
        elif datetime.strptime("1970-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("1980-01-01", "'%Y-%m-%d'"):
            return "1980s"
        elif datetime.strptime("1960-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("1970-01-01", "'%Y-%m-%d'"):
            return "1970s"
        elif datetime.strptime("1950-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("1960-01-01", "'%Y-%m-%d'"):
            return "1960s"
        elif datetime.strptime("1940-01-01", "'%Y-%m-%d'") <= week < datetime.strptime("1950-01-01", "'%Y-%m-%d'"):
            return "1950s"
        else:
            return "1940s"
