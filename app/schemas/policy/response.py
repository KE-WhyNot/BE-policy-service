from typing import Generic, TypeVar, List
from pydantic import BaseModel

T = TypeVar('T')

class Meta(BaseModel):
    count: int

class ListResponse(BaseModel, Generic[T]):
    meta: Meta
    data: List[T]