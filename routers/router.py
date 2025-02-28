from fastapi import APIRouter, UploadFile, File, Form
from typing import Dict
from services.file_reader import FileReader
from services.validator import Validator
from config.rules import VALIDATION_RULES
from schemas.response import ValidationResponse


router = APIRouter()
file_reader = FileReader()
validator = Validator(VALIDATION_RULES)


@router.post("/validate/", response_model=ValidationResponse)
async def validate_excel(file: UploadFile = File(...), id: str = Form(...)) -> Dict:
    """API nhận file Excel và ID, sau đó xác thực dữ liệu và trả về JSON."""
    df = await file_reader.read_excel(file)
    print(df.shape)
    return validator.validate(df, id)

