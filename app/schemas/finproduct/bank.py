from pydantic import BaseModel

class Bank(BaseModel):
    id: int
    top_fin_grp_no: str
    fin_co_no: str
    kor_co_nm: str
    nickname: str
