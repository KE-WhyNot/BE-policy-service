from pydantic import BaseModel

class SpecialType(BaseModel):
    id : int
    name : str
    db_row_name : str