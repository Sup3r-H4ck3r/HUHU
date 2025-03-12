"""Schema definitions for validation responses."""
from typing import Dict, List, Any

from pydantic import BaseModel, Field


class ValidationResponse(BaseModel):
    """Response schema for validation results.
    
    Attributes:
        errors: Dictionary mapping row indices to lists of invalid column names
        data: List of records after validation (empty if validation failed)
    """
    errors: Dict[int, List[str]] = Field(
        default_factory=dict,
        description="Dictionary mapping row indices to lists of invalid column names"
    )
    data: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of records after validation (empty if validation failed)"
    )