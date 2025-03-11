import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from typing import Any, List, Tuple, Optional

class DatabaseHelper:
    _pool = None  # Connection pool dùng chung cho tất cả instance
    
    @classmethod
    def initialize_pool(cls, minconn: int, maxconn: int, connection_string: str):
        """Khởi tạo connection pool"""
        if cls._pool is None:
            cls._pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, connection_string)

    def __init__(self, connection_string: str, use_pool: bool = True):
        """
        Khởi tạo DatabaseHelper.
        Nếu use_pool = True, sử dụng connection pool nếu có.
        Nếu use_pool = False, mở kết nối trực tiếp.
        """
        self.connection_string = connection_string
        self.use_pool = use_pool
        self.conn: Optional[psycopg2.extensions.connection] = None
    
    def open_connection(self) -> str:
        """Lấy kết nối từ pool nếu dùng pool, hoặc mở kết nối mới nếu không"""
        if self.conn:
            return "Connection is already open."
        
        try:
            if self.use_pool:
                if not DatabaseHelper._pool:
                    return "Connection pool is not initialized."
                self.conn = DatabaseHelper._pool.getconn()
            else:
                self.conn = psycopg2.connect(self.connection_string)
            return "Connection acquired successfully."
        except Exception as e:
            return f"Error acquiring connection: {str(e)}"
    

    def get_procedure_param_types(self, procedure_name: str) -> List[Tuple[int, str, str]]:

        """ Lấy danh sách (thứ tự, tên tham số, kiểu dữ liệu) của stored procedure """

        query = """
        SELECT t.typname AS param_type
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN LATERAL unnest(proargnames, proargtypes::oid[]) 
            WITH ORDINALITY AS param(param_name, type_oid, ordinality) 
            ON true
        JOIN pg_type t ON param.type_oid = t.oid
        WHERE p.proname = %s
        ORDER BY param.ordinality;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (procedure_name,))
                result = [i[0] for i in cursor.fetchall()]
                print(result)
                return result
        except Exception as e:
            return []

    # def execute_scalar_s_procedure_x(self, sprocedure_name: str, *params: Any) -> Tuple[Any, str]:
    #     """ Gọi stored procedure mà không cần chỉ định kiểu dữ liệu trước """
    #     if not self.conn:
    #         self.open_connection()
        
    #     expected_types = self.get_procedure_param_types(sprocedure_name)
    #     if not expected_types:
    #         return None, f"Không lấy được kiểu dữ liệu của {sprocedure_name}"
        
    #     try:
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(f"SELECT {sprocedure_name}({', '.join([f'%s::{param_type}' for param_type in expected_types])})", params)
    #             self.conn.commit()
    #             result = cursor.fetchone()
    #         return (result[0] if result else None, "")
    #     except Exception as e:
    #         return None, f"Lỗi khi gọi stored procedure: {str(e)}"

    def close_connection(self) -> str:
        """Trả kết nối về pool nếu dùng pool, hoặc đóng kết nối nếu không"""
        if self.conn:
            try:
                if self.use_pool:
                    DatabaseHelper._pool.putconn(self.conn)
                else:
                    self.conn.close()
                self.conn = None
                return "Connection closed successfully."
            except Exception as e:
                return f"Error closing connection: {str(e)}"
        return "No active connection to close."
    
    def execute_non_query(self, query: str) -> str:
        """ Thực thi câu lệnh không trả về kết quả (INSERT, UPDATE, DELETE) """
        if not self.conn:
            self.open_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()
            return "Query executed successfully."
        except Exception as e:
            return f"Error executing query: {str(e)}"

    def execute_scalar(self, query: str) -> Tuple[Any, str]:
        """ Thực thi câu lệnh trả về một giá trị duy nhất """
        if not self.conn:
            self.open_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()
                result = cursor.fetchone()
            return (result[0] if result else None, "")
        except Exception as e:
            return None, f"Error executing scalar query: {str(e)}"

    def execute_s_procedure(self, sprocedure_name: str, *params: Any) -> str:
        """ Gọi stored procedure không trả về dữ liệu """
        if not self.conn:
            self.open_connection()
        
        expected_types = self.get_procedure_param_types(sprocedure_name)
        if not expected_types:
            return None, f"Không lấy được kiểu dữ liệu của {sprocedure_name}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT {sprocedure_name}({', '.join([f'%s::{expected_types[i]}' for i in range(len(params))])})", params)
                self.conn.commit()
            return "Stored procedure executed successfully."
        except Exception as e:
            return f"Error executing stored procedure: {str(e)}"

    def execute_scalar_s_procedure(self, sprocedure_name: str, *params: Any) -> Tuple[Any, str]:
        """ Gọi stored procedure trả về một giá trị duy nhất """
        if not self.conn:
            self.open_connection()
        expected_types = self.get_procedure_param_types(sprocedure_name)
        if not expected_types:
            return None, f"Không lấy được kiểu dữ liệu của {sprocedure_name}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT {sprocedure_name}({', '.join([f'%s::{expected_types[i]}' for i in range(len(params))])})", params)
                self.conn.commit()
                result = cursor.fetchone()
            return (result[0] if result else None, "")
        except Exception as e:
            return None, f"Error executing scalar stored procedure: {str(e)}"

    def execute_s_procedure_return_data_table(self, sprocedure_name: str, *params: Any) -> Tuple[List[dict], str]:
        """ Gọi stored procedure trả về bảng dữ liệu """
        if not self.conn:
            self.open_connection()
        expected_types = self.get_procedure_param_types(sprocedure_name)
        if not expected_types:
            return None, f"Không lấy được kiểu dữ liệu của {sprocedure_name}"
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT * FROM {sprocedure_name}({', '.join([f'%s::{expected_types[i]}' for i in range(len(params))])})", params)
                self.conn.commit()
                return cursor.fetchall(), ""
        except Exception as e:
            return [], f"Error executing stored procedure: {str(e)}"
    
    @classmethod
    def close_pool(cls):
        """Đóng toàn bộ pool khi không cần nữa"""
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
