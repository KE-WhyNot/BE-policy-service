from pydantic import BaseModel

class Specialization(BaseModel):
    id: int
    name: str
    code: str | None = None
    is_active: bool