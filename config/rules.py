"""Configuration module for validation rules."""
from typing import Dict, Any, Union, Type

import pandas as pd

# Type alias for validation rule type specifications
ValidationDataType = Union[Type, str]

# Type alias for validation rule structure
ValidationRule = Dict[str, Dict[str, ValidationDataType]]

# Validation rules configuration
VALIDATION_RULES: ValidationRule = {
    "1": {
        "dtype": {
            "Mã thuế": str,
            "Tên thuế": str,
            "Phần trăm thuế": int,
            "test1": "datetime",
            "test2": int,
            "test3": int,
            "test4": int,
            "test5": int,
            "test6": int
        },
    },
    "2": {
        "dtype": {
            "Mã Thuế": str,
            "Phần Trăm Thuế": float
        }
    }
}