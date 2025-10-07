from pydantic import BaseModel

class Category(BaseModel):
    id: int
    code: str
    name: str
    parent_id :int | None = None
    level: str
    is_active: bool