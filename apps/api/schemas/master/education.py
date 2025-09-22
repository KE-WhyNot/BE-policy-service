from pydantic import BaseModel

class Education(BaseModel):
    id: int
    name: str
    code: str | None = None
    is_active: bool