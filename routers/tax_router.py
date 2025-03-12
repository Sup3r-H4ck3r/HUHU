"""FastAPI router for tax-related endpoints."""
from fastapi import APIRouter, HTTPException, Depends, status
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from data.database_helper import DatabaseHelper
from services.tax_service import create_tax_service, TaxService
from schemas.tax import TaxCreate, TaxUpdate, TaxDelete, TaxSearch
from config.config import DATABASE_URL, MAX_CONN, MIN_CONN, USE_POOL


# Response models for better API documentation
class SuccessResponse(BaseModel):
    """Standard success response model."""
    message: str
    result: Optional[Any] = None


class ErrorResponse(BaseModel):
    """Standard error response model."""
    message: str
    detail: str


# Initialize database helper
db_helper = DatabaseHelper(connection_string=DATABASE_URL, use_pool=USE_POOL)
if USE_POOL:
    db_helper.initialize_pool(MIN_CONN, MAX_CONN, DATABASE_URL)


# Dependency for getting TaxService
def get_tax_service() -> TaxService:
    """Dependency to provide TaxService instance."""
    return create_tax_service(db_helper)


# Create router with prefix and tags for better API documentation
router = APIRouter(
    prefix="/tax",
    tags=["Tax Management"],
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse}
    }
)


@router.post(
    "/create",
    response_model=SuccessResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create new tax code references",
    description="Create multiple tax code references from a list of tax data."
)
async def create_tax(
    data: List[TaxCreate],
    tax_service: TaxService = Depends(get_tax_service)
) -> Dict[str, Any]:
    """Create new tax code references from JSON data.
    
    Args:
        data: List of tax data to create
        tax_service: TaxService instance from dependency
        
    Returns:
        Response with success message or error details
        
    Raises:
        HTTPException: If an error occurs during processing
    """
    try:
        errors = []
        for item in data:
            result = tax_service.create_tax_code_ref(**item.dict())
            if isinstance(result, tuple) and result[1]:
                errors.append(result[1])
            elif isinstance(result, str) and result:
                errors.append(result)
                
        if not errors:
            return {"message": "Created successfully"}
        return {"message": "error", "result": errors}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.put(
    "/update",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="Update tax code references",
    description="Update multiple tax code references from a list of tax data."
)
async def update_tax(
    data: List[TaxUpdate],
    tax_service: TaxService = Depends(get_tax_service)
) -> Dict[str, Any]:
    """Update tax code references from JSON data.
    
    Args:
        data: List of tax data to update
        tax_service: TaxService instance from dependency
        
    Returns:
        Response with success message or error details
        
    Raises:
        HTTPException: If an error occurs during processing
    """
    try:
        errors = []
        for item in data:
            result = tax_service.update_tax_code_ref(**item.dict())
            if isinstance(result, tuple) and result[1]:
                errors.append(result[1])
            elif isinstance(result, str) and result:
                errors.append(result)
                
        if not errors:
            return {"message": "Updated successfully"}
        return {"message": "error", "result": errors}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete(
    "/delete",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="Delete tax code references",
    description="Delete multiple tax code references by ID list."
)
async def delete_tax(
    delete_data: TaxDelete,
    tax_service: TaxService = Depends(get_tax_service)
) -> Dict[str, Any]:
    """Delete multiple tax code references by ID list.
    
    Args:
        delete_data: Data containing IDs to delete and user performing deletion
        tax_service: TaxService instance from dependency
        
    Returns:
        Response with deletion results
    """
    result, error = tax_service.delete_multi_tax_code_ref(
        delete_data.json_list_id,
        delete_data.updated_by
    )
    
    if error:
        return {"message": "error", "result": error}
    return {"message": "Deleted successfully", "result": result}


@router.get(
    "/tax_code_rcd",
    status_code=status.HTTP_200_OK,
    summary="Get tax code reference by ID",
    description="Retrieve detailed information about a tax code reference by its ID."
)
async def get_tax_by_id(
    tax_code_rcd: str,
    tax_service: TaxService = Depends(get_tax_service)
) -> Dict[str, Any]:
    """Get tax code reference details by ID.
    
    Args:
        tax_code_rcd: Tax code record ID
        tax_service: TaxService instance from dependency
        
    Returns:
        Tax code reference details
        
    Raises:
        HTTPException: If record not found or error occurs
    """
    result, error = tax_service.get_tax_code_ref_by_id(tax_code_rcd)
    
    if error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error
        )
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tax code reference with ID {tax_code_rcd} not found"
        )
        
    return {"data": result}


@router.get(
    "/dropdown",
    status_code=status.HTTP_200_OK,
    summary="Get tax dropdown options",
    description="Retrieve tax code references formatted for dropdown selection."
)
async def get_tax_dropdown(
    lang: str,
    tax_service: TaxService = Depends(get_tax_service)
) -> Dict[str, Any]:
    """Get tax code references for dropdown selection.
    
    Args:
        lang: Language code
        tax_service: TaxService instance from dependency
        
    Returns:
        List of tax code references formatted for dropdown
    """
    result, error = tax_service.get_tax_code_ref_dropdown(lang)
    
    if error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error
        )
        
    return {"data": result}


@router.get(
    "/search",
    status_code=status.HTTP_200_OK,
    summary="Search tax code references",
    description="Search tax code references with optional filters and pagination."
)
async def search_tax(
    search_params: TaxSearch = Depends(),
    tax_service: TaxService = Depends(get_tax_service)
) -> Dict[str, Any]:
    """Search tax code references with filters and pagination.
    
    Args:
        search_params: Search parameters from query string
        tax_service: TaxService instance from dependency
        
    Returns:
        Search results with pagination
    """
    result, error = tax_service.search_tax_code_ref(
        search_params.page_index,
        search_params.page_size,
        search_params.lang,
        search_params.tax_code_rcd or "",
        search_params.tax_rule_rcd or "",
        search_params.tax_code_ref_name or ""
    )
    
    if error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error
        )
        
    return {"data": result}