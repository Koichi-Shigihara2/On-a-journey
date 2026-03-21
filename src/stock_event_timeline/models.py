from typing import List, Optional
from pydantic import BaseModel


class EventModel(BaseModel):
    code: str
    title: str
    comment: str
    categories: List[str]
    causality_confidence: str
    alternative_factors: List[str]
    is_main_cause: bool
    window_start: str
    window_end: str


class PricePoint(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int
