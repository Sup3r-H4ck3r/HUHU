from pydantic import BaseModel
from typing import Optional


class TaxCreate(BaseModel):
    tax_code_rcd: str
    tax_rule_rcd: str
    tax_code_ref_name_e: str
    tax_code_ref_name_l: str
    seq_num: int
    must_not_change_flag: bool
    user_defined_rate_flag: bool
    created_by_user_id: str
    tax_rate: float


class TaxUpdate(TaxCreate):
    status: int
    lu_user_id: str


class TaxDelete(BaseModel):
    json_list_id: str
    updated_by: str


class TaxSearch(BaseModel):
    page_index: int
    page_size: int
    lang: str
    tax_code_rcd: Optional[str] = None
    tax_rule_rcd: Optional[str] = None
    tax_code_ref_name: Optional[str] = None
