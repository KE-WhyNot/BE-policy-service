from pydantic import BaseModel

class SpecialCondition(BaseModel):
    id : int
    name : str
    db_row_name : str