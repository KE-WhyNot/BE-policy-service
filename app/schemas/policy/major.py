from pydantic import BaseModel

class Major(BaseModel):
    id: int
    name: str
    code: str | None = None
    is_active: bool