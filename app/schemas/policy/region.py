from pydantic import BaseModel

class Region(BaseModel):
    no: int | None = None  # 순서 번호 (API 응답에서만 사용)
    id: int
    code: str
    name: str
    parent_id :int | None = None
    kind: str
    zip_code: str | None = None
    is_active: bool