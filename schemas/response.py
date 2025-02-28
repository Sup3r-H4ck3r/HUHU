from pydantic import BaseModel
from typing import Dict, List


class ValidationResponse(BaseModel):
    errors: Dict[int, List[str]]
