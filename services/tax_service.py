from data.DatabaseHelper import DatabaseHelper
from typing import List, Dict, Tuple, Any
import numpy as np


class TaxService:
    def __init__(self, db_helper: DatabaseHelper):
        self.db = db_helper

    def create_tax_code_ref(
        self, p_tax_code_rcd: str, p_tax_rule_rcd: str, p_tax_code_ref_name_e: str, 
        p_tax_code_ref_name_l: str, p_seq_num: int, p_must_not_change_flag: bool, 
        p_user_defined_rate_flag: bool, p_created_by_user_id: str, p_tax_rate: float
    ) -> Tuple[str, str]:
        """ Gọi stored procedure tạo mới tax_code_ref """
        expected_types = ['varchar']*4 + ['smallint', 'boolean', 'boolean', 'uuid', 'numeric']
        result, error = self.db.execute_scalar_s_procedure(
            "sp_tax_code_ref_create", p_tax_code_rcd, p_tax_rule_rcd,
            p_tax_code_ref_name_e, p_tax_code_ref_name_l, p_seq_num,
            p_must_not_change_flag, p_user_defined_rate_flag, p_created_by_user_id, p_tax_rate, expected_types=expected_types
        )
        if error=="":
            return result
        return error

    def update_tax_code_ref(
        self, p_tax_code_rcd: str, p_tax_rule_rcd: str, p_tax_code_ref_name_e: str, 
        p_tax_code_ref_name_l: str, p_seq_num: int, p_must_not_change_flag: bool, 
        p_user_defined_rate_flag: bool, p_lu_user_id: str, p_tax_rate: float, p_status: int
    ) -> Tuple[str, str]:
        """ Gọi stored procedure cập nhật tax_code_ref """
        expected_types = ['varchar']*4 + ['smallint'] + ['boolean']*2 + ['uuid', 'numeric', 'smallint']
        result, error = self.db.execute_scalar_s_procedure(
            "sp_tax_code_ref_update", p_tax_code_rcd, p_tax_rule_rcd, 
            p_tax_code_ref_name_e, p_tax_code_ref_name_l, p_seq_num, 
            p_must_not_change_flag, p_user_defined_rate_flag, p_lu_user_id, p_tax_rate, p_status, expected_types=expected_types
        )
        if error=="":
            return result
        return error

    def search_tax_code_ref(
        self, p_page_index: int, p_page_size: int, lang: str,
        p_tax_code_rcd: str = None, p_tax_rule_rcd: str = None,
        p_tax_code_ref_name: str = None
    ) -> Tuple[List[Dict[str, Any]], str]:
        """ Gọi stored procedure tìm kiếm tax_code_ref """
        expected_types = ['interger']*2 + ['char'] + ['varchar']*2
        result, error = self.db.execute_s_procedure_return_data_table(
            "sp_tax_code_ref_search", p_page_index, p_page_size, lang, 
            p_tax_code_rcd, p_tax_rule_rcd, p_tax_code_ref_name, expected_types=expected_types
        )
        if error=="":
            return result
        return error

    def get_tax_code_ref_dropdown(self, lang: str) -> Tuple[List[Dict[str, Any]], str]:
        """ Gọi stored procedure lấy danh sách dropdown của tax_code_ref """
        expected_types = ['varchar']
        result, error = self.db.execute_s_procedure_return_data_table("sp_tax_code_ref_get_list_dropdown", lang, expected_types=expected_types)
        if error=="":
            return result
        return error

    def get_tax_code_ref_by_id(self, tax_code_rcd: str) -> Tuple[List[Dict[str, Any]], str]:
        """ Gọi stored procedure lấy thông tin tax_code_ref theo ID """
        expected_types = ['varchar']
        result, error = self.db.execute_s_procedure_return_data_table("sp_tax_code_ref_get_by_id", tax_code_rcd, expected_types=expected_types)
        if error=="":
            return result
        return error
       

    def delete_multi_tax_code_ref(self, p_json_list_id: str, p_updated_by: str) -> Tuple[List[Dict[str, Any]], str]:
        """ Gọi stored procedure xóa nhiều tax_code_ref """
        expected_types = ['json', 'uuid']
        result, error = self.db.execute_s_procedure_return_data_table("sp_tax_code_ref_delete_multi", p_json_list_id, p_updated_by, expected_types=expected_types)
        if error=="":
            return result
        return error



