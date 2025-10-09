from pydantic import BaseModel

class Keyword(BaseModel):
    id: int
    name: str
    # code: str | None = None
    is_active: bool