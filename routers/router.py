from fastapi import APIRouter, HTTPException
from typing import List, Dict
from tqdm import tqdm
from data.DatabaseHelper import DatabaseHelper
from services.tax_service import TaxService
from config.config import DATABASE_URL, MAX_CONN, MIN_CONN, USE_POOL

# Khởi tạo FastAPI router
router = APIRouter()

# Kết nối database
db_helper = DatabaseHelper(connection_string=DATABASE_URL, use_pool=USE_POOL)
if USE_POOL:
    db_helper.initialize_pool(MIN_CONN, MAX_CONN, DATABASE_URL)
    
tax_service = TaxService(db_helper)


@router.post("/tax/create")
async def create_tax(data: List[Dict]):
    """Tạo mới danh sách thuế từ JSON."""
    try:
        mgerr = []
        for item in tqdm(data):
            err = tax_service.create_tax_code_ref(**item)
            if err != "":
                mgerr.append(err)
        # result = [tax_service.create_tax_code_ref(**item) for item in tqdm(data)]
        if mgerr == []:
            return {"message": "Created successfully"}
        return {"message": "error", "result": err}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tax/update")
async def update_tax(data: List[Dict]):
    """Cập nhật danh sách mã thuế từ JSON."""
    try:
        mgerr = []
        for item in tqdm(data):
            err = tax_service.update_tax_code_ref(**item)
            if err != "":
                mgerr.append(err)
        if mgerr == []:
            return {"message": "Created successfully"}
        return {"message": "error", "result": err}
        # result = [tax_service.update_tax_code_ref(**item) for item in tqdm(data)]
        # return {"message": "Updated successfully", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tax/delete")
async def delete_tax(p_json_list_id: str, p_updated_by: str):
    """Xóa nhiều mã thuế theo danh sách ID."""
    return tax_service.delete_multi_tax_code_ref(p_json_list_id, p_updated_by)


@router.get("/tax/tax_code_rcd")
async def get_tax_by_id(tax_code_rcd: str):
    """Lấy thông tin thuế theo mã."""
    return tax_service.get_tax_code_ref_by_id(tax_code_rcd)


@router.get("/tax/dropdown")
async def get_tax_dropdown(lang: str):
    """Lấy danh sách thuế dạng dropdown."""
    return tax_service.get_tax_code_ref_dropdown(lang)


@router.get("/tax/search")
async def search_tax(p_page_index: int, p_page_size: int, lang: str, 
                     p_tax_code_rcd: str = "", p_tax_rule_rcd: str = "", p_tax_code_ref_name: str = ""):
    """Tìm kiếm mã thuế theo tiêu chí."""
    return tax_service.search_tax_code_ref(p_page_index, p_page_size, lang, p_tax_code_rcd, p_tax_rule_rcd, p_tax_code_ref_name)
