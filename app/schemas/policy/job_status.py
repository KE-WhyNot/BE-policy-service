from pydantic import BaseModel

class JobStatus(BaseModel):
    id: int
    name: str
    code: str | None = None
    is_active: bool