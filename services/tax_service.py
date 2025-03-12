"""Tax service module implementing repository pattern for tax operations."""
from typing import List, Dict, Tuple, Any, Union, Optional
from abc import ABC, abstractmethod

from data.database_helper import DatabaseHelper


class ITaxRepository(ABC):
    """Interface for tax repository operations."""
    
    @abstractmethod
    def create(self, **kwargs) -> Tuple[Optional[str], str]:
        """Create a new tax code reference."""
        pass
    
    @abstractmethod
    def update(self, **kwargs) -> Tuple[Optional[str], str]:
        """Update an existing tax code reference."""
        pass
    
    @abstractmethod
    def delete_multiple(self, json_list_id: str, updated_by: str) -> Tuple[List[Dict[str, Any]], str]:
        """Delete multiple tax code references."""
        pass
    
    @abstractmethod
    def get_by_id(self, tax_code_rcd: str) -> Tuple[List[Dict[str, Any]], str]:
        """Get tax code reference by ID."""
        pass
    
    @abstractmethod
    def get_dropdown(self, lang: str) -> Tuple[List[Dict[str, Any]], str]:
        """Get list of tax code references for dropdown."""
        pass
    
    @abstractmethod
    def search(self, **kwargs) -> Tuple[List[Dict[str, Any]], str]:
        """Search for tax code references."""
        pass


class TaxRepository(ITaxRepository):
    """Implementation of tax repository using stored procedures."""
    
    def __init__(self, db_helper: DatabaseHelper):
        """Initialize with database helper.
        
        Args:
            db_helper: DatabaseHelper instance
        """
        self.db = db_helper
    
    def create(self, **kwargs) -> Tuple[Optional[str], str]:
        """Create a new tax code reference.
        
        Args:
            tax_code_rcd: Tax code record ID
            tax_rule_rcd: Tax rule record ID
            tax_code_ref_name_e: English reference name
            tax_code_ref_name_l: Local reference name
            seq_num: Sequence number
            must_not_change_flag: Flag indicating if record cannot be changed
            user_defined_rate_flag: Flag indicating if user can define rate
            created_by_user_id: User ID of creator
            tax_rate: Tax rate value
            
        Returns:
            Tuple containing (result, error_message)
        """
        result, error = self.db.execute_scalar_stored_procedure(
            "sp_tax_code_ref_create",
            kwargs.get('tax_code_rcd'),
            kwargs.get('tax_rule_rcd'),
            kwargs.get('tax_code_ref_name_e'),
            kwargs.get('tax_code_ref_name_l'),
            kwargs.get('seq_num'),
            kwargs.get('must_not_change_flag'),
            kwargs.get('user_defined_rate_flag'),
            kwargs.get('created_by_user_id'),
            kwargs.get('tax_rate')
        )
        if not error:
            return result, ""
        return None, error
    
    def update(self, **kwargs) -> Tuple[Optional[str], str]:
        """Update an existing tax code reference.
        
        Args:
            tax_code_rcd: Tax code record ID
            tax_rule_rcd: Tax rule record ID
            tax_code_ref_name_e: English reference name
            tax_code_ref_name_l: Local reference name
            seq_num: Sequence number
            must_not_change_flag: Flag indicating if record cannot be changed
            user_defined_rate_flag: Flag indicating if user can define rate
            lu_user_id: Last update user ID
            tax_rate: Tax rate value
            status: Status code
            
        Returns:
            Tuple containing (result, error_message)
        """
        result, error = self.db.execute_scalar_stored_procedure(
            "sp_tax_code_ref_update",
            kwargs.get('tax_code_rcd'),
            kwargs.get('tax_rule_rcd'),
            kwargs.get('tax_code_ref_name_e'),
            kwargs.get('tax_code_ref_name_l'),
            kwargs.get('seq_num'),
            kwargs.get('must_not_change_flag'),
            kwargs.get('user_defined_rate_flag'),
            kwargs.get('lu_user_id'),
            kwargs.get('tax_rate'),
            kwargs.get('status')
        )
        if not error:
            return result, ""
        return None, error
    
    def delete_multiple(self, json_list_id: str, updated_by: str) -> Tuple[List[Dict[str, Any]], str]:
        """Delete multiple tax code references.
        
        Args:
            json_list_id: JSON string containing list of IDs to delete
            updated_by: User ID of person performing deletion
            
        Returns:
            Tuple containing (result, error_message)
        """
        result, error = self.db.execute_stored_procedure_return_data(
            "sp_tax_code_ref_delete_multi", 
            json_list_id, 
            updated_by
        )
        if not error:
            return result, ""
        return [], error
    
    def get_by_id(self, tax_code_rcd: str) -> Tuple[List[Dict[str, Any]], str]:
        """Get tax code reference by ID.
        
        Args:
            tax_code_rcd: Tax code record ID
            
        Returns:
            Tuple containing (result, error_message)
        """
        result, error = self.db.execute_stored_procedure_return_data(
            "sp_tax_code_ref_get_by_id", 
            tax_code_rcd
        )
        if not error:
            return result, ""
        return [], error
    
    def get_dropdown(self, lang: str) -> Tuple[List[Dict[str, Any]], str]:
        """Get list of tax code references for dropdown.
        
        Args:
            lang: Language code
            
        Returns:
            Tuple containing (result, error_message)
        """
        result, error = self.db.execute_stored_procedure_return_data(
            "sp_tax_code_ref_get_list_dropdown", 
            lang
        )
        if not error:
            return result, ""
        return [], error

    def search(self, **kwargs) -> Tuple[List[Dict[str, Any]], str]:
        """Search for tax code references.
  
        Args:
            page_index: Page index for pagination
            page_size: Page size for pagination
            lang: Language code
            tax_code_rcd: Optional tax code record ID filter
            tax_rule_rcd: Optional tax rule record ID filter
            tax_code_ref_name: Optional reference name filter
            
        Returns:
            Tuple containing (result, error_message)
        """
        result, error = self.db.execute_stored_procedure_return_data(
            "sp_tax_code_ref_search",
            kwargs.get('page_index'),
            kwargs.get('page_size'),
            kwargs.get('lang'),
            kwargs.get('tax_code_rcd'),
            kwargs.get('tax_rule_rcd'),
            kwargs.get('tax_code_ref_name')
        )
        if not error:
            return result, ""
        return [], error


class TaxService:
    """Service class for tax operations using repository pattern."""
    
    def __init__(self, tax_repository: ITaxRepository):
        """Initialize with tax repository.
        
        Args:
            tax_repository: Repository implementing ITaxRepository interface
        """
        self.repository = tax_repository
    
    def create_tax_code_ref(self, **kwargs) -> Union[str, Tuple[Optional[str], str]]:
        """Create a new tax code reference.
        
        Returns:
            Result or error message
        """
        return self.repository.create(**kwargs)
    
    def update_tax_code_ref(self, **kwargs) -> Union[str, Tuple[Optional[str], str]]:
        """Update an existing tax code reference.
        
        Returns:
            Result or error message
        """
        return self.repository.update(**kwargs)
    
    def delete_multi_tax_code_ref(self, p_json_list_id: str, p_updated_by: str) -> Tuple[List[Dict[str, Any]], str]:
        """Delete multiple tax code references.
        
        Args:
            p_json_list_id: JSON string containing list of IDs to delete
            p_updated_by: User ID of person performing deletion
            
        Returns:
            Tuple containing (result, error_message)
        """
        return self.repository.delete_multiple(p_json_list_id, p_updated_by)
    
    def get_tax_code_ref_by_id(self, tax_code_rcd: str) -> Tuple[List[Dict[str, Any]], str]:
        """Get tax code reference by ID.
        
        Args:
            tax_code_rcd: Tax code record ID
            
        Returns:
            Tuple containing (result, error_message)
        """
        return self.repository.get_by_id(tax_code_rcd)
    
    def get_tax_code_ref_dropdown(self, lang: str) -> Tuple[List[Dict[str, Any]], str]:
        """Get list of tax code references for dropdown.
        
        Args:
            lang: Language code
            
        Returns:
            Tuple containing (result, error_message)
        """
        return self.repository.get_dropdown(lang)
    
    def search_tax_code_ref(
        self, 
        page_index: int, 
        page_size: int, 
        lang: str,
        tax_code_rcd: Optional[str] = None, 
        tax_rule_rcd: Optional[str] = None,
        tax_code_ref_name: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Search for tax code references.
        
        Args:
            page_index: Page index for pagination
            page_size: Page size for pagination
            lang: Language code
            tax_code_rcd: Optional tax code record ID filter
            tax_rule_rcd: Optional tax rule record ID filter
            tax_code_ref_name: Optional reference name filter
            
        Returns:
            Tuple containing (result, error_message)
        """
        return self.repository.search(
            page_index=page_index,
            page_size=page_size,
            lang=lang,
            tax_code_rcd=tax_code_rcd,
            tax_rule_rcd=tax_rule_rcd,
            tax_code_ref_name=tax_code_ref_name
        )


# Factory function to create TaxService with proper dependencies
def create_tax_service(db_helper: DatabaseHelper) -> TaxService:
    """Create a TaxService instance with proper repository.
    
    Args:
        db_helper: DatabaseHelper instance
        
    Returns:
        Configured TaxService instance
    """
    repository = TaxRepository(db_helper)
    return TaxService(repository)