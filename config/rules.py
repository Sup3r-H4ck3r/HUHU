from typing import Dict, Any
import pandas as pd

VALIDATION_RULES: Dict[str, Dict[str, Any]] = {
    "1": {
        "dtype": {"Mã thuế": str, "Tên thuế": str, "Phần trăm thuế": int, "test1": "datetime", "test2": int, "test3": int, "test4": int, "test5": int, "test6": int},
    },
}
