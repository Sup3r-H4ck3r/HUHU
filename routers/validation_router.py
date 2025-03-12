"""API routes for data validation."""
import logging
from typing import Dict, Any

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status

from services.file_readers import FileReader
from services.validators import Validator
from config.rules import VALIDATION_RULES
from schemas.validation_response import ValidationResponse

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/api/v1/validate",
    tags=["validation"],
    responses={404: {"description": "Not found"}},
)

# Dependencies
def get_validator() -> Validator:
    """Dependency to get validator instance."""
    return Validator(VALIDATION_RULES)

def get_file_reader() -> FileReader:
    """Dependency to get file reader instance."""
    return FileReader()


@router.post(
    "/{rule_id}",
    response_model=ValidationResponse,
    status_code=status.HTTP_200_OK,
    summary="Validate uploaded Excel file",
    description="Upload Excel file and validate it against specified rule set"
)
async def validate_excel(
    rule_id: str,
    file: UploadFile = File(...),
    validator: Validator = Depends(get_validator),
    file_reader: FileReader = Depends(get_file_reader)
) -> ValidationResponse:
    """Validate uploaded Excel file against specified rule set.
    
    Args:
        rule_id: ID of validation rule set to apply
        file: Uploaded Excel file
        validator: Validator instance (injected)
        file_reader: FileReader instance (injected)
        
    Returns:
        ValidationResponse object with validation results
        
    Raises:
        HTTPException: For invalid rule_id, file format, or missing columns
    """
    try:
        # Check file extension
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only Excel files (.xlsx, .xls) are supported"
            )
        
        # Read file
        logger.info(f"Reading file {file.filename}")
        df = await file_reader.read_excel(file)
        
        # Validate data
        logger.info(f"Validating file against rule {rule_id}")
        result = validator.validate(df, rule_id)
        
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Log and wrap other exceptions
        logger.exception(f"Error during validation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation error: {str(e)}"
        )