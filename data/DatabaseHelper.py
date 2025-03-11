# import psycopg2
# from psycopg2.extras import RealDictCursor
# from typing import Any, List, Tuple


# class DatabaseHelper:
#     def __init__(self, connection_string: str):
#         self.connection_string = connection_string
#         self.conn = None

#     def open_connection(self) -> str:
#         """ Mở kết nối nếu chưa có """
#         if self.conn:
#             return "Connection is already open."
#         try:
#             self.conn = psycopg2.connect(self.connection_string)
#             return "Connection opened successfully."
#         except Exception as e:
#             return f"Error opening connection: {str(e)}"
        

#     def close_connection(self) -> str:
#         """ Đóng kết nối """
#         if self.conn:
#             self.conn.close()
#             self.conn = None
#             return "Connection closed successfully."
#         return "No active connection to close."

#     def execute_non_query(self, query: str) -> str:
#         """ Thực thi câu lệnh không trả về kết quả (INSERT, UPDATE, DELETE) """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(query)
#                 self.conn.commit()
#             return "Query executed successfully."
#         except Exception as e:
#             return f"Error executing query: {str(e)}"

#     def execute_scalar(self, query: str) -> Tuple[Any, str]:
#         """ Thực thi câu lệnh trả về một giá trị duy nhất """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(query)
#                 result = cursor.fetchone()
#             return (result[0] if result else None), ""
#         except Exception as e:
#             return None, f"Error executing scalar query: {str(e)}"

#     def execute_s_procedure(self, sprocedure_name: str, *params: Any) -> str:
#         """ Gọi stored procedure không trả về dữ liệu """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(f"CALL {sprocedure_name}({', '.join(['%s'] * len(params))})", params)
#                 self.conn.commit()
#             return "Stored procedure executed successfully."
#         except Exception as e:
#             return f"Error executing stored procedure: {str(e)}"

#     def execute_scalar_s_procedure(self, sprocedure_name: str, *params: Any) -> Tuple[Any, str]:
#         """ Gọi stored procedure trả về một giá trị duy nhất """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(f"SELECT {sprocedure_name}({', '.join(['%s'] * len(params))})", params)
#                 result = cursor.fetchone()
#             return (result[0] if result else None), ""
#         except Exception as e:
#             return None, f"Error executing scalar stored procedure: {str(e)}"

#     def execute_s_procedure_return_data_table(self, func_name: str, *params: Any) -> Tuple[List[dict], str]:
#         """ Gọi stored procedure trả về bảng dữ liệu """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 cursor.execute(f"SELECT * FROM {func_name}({', '.join(['%s'] * len(params))})", params)
#                 return cursor.fetchall(), ""
#         except Exception as e:
#             return [], f"Error executing stored procedure: {str(e)}"

#     def reader_s_procedure(self, func_name: str, *params: Any) -> Tuple[List[dict], str]:
#         """ Đọc dữ liệu từ stored procedure """
#         return self.execute_s_procedure_return_data_table(func_name, *params)

# -----------------------------------------------------------------------------------------------------------------------------
# import psycopg2
# from psycopg2.extras import RealDictCursor
# from typing import Any, List, Tuple


# class DatabaseHelper:
#     def __init__(self, connection_string: str):
#         self.connection_string = connection_string
#         self.conn = None

#     def __enter__(self):
#         """ Cho phép dùng `with DatabaseHelper(...)` """
#         self.open_connection()
#         return self

#     def __exit__(self, exc_type, exc_value, traceback):
#         """ Đóng kết nối khi thoát khỏi `with` """
#         self.close_connection()

#     def open_connection(self) -> str:
#         """ Mở kết nối nếu chưa có """
#         if self.conn:
#             return "Connection is already open."
#         try:
#             self.conn = psycopg2.connect(self.connection_string)
#             return "Connection opened successfully."
#         except Exception as e:
#             return f"Error opening connection: {str(e)}"

#     def close_connection(self) -> str:
#         """ Đóng kết nối """
#         if self.conn:
#             self.conn.close()
#             self.conn = None
#             return "Connection closed successfully."
#         return "No active connection to close."

#     def execute_non_query(self, query: str) -> str:
#         """ Thực thi câu lệnh không trả về kết quả (INSERT, UPDATE, DELETE) """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(query)
#                 self.conn.commit()
#             return "Query executed successfully."
#         except Exception as e:
#             return f"Error executing query: {str(e)}"

#     def execute_scalar(self, query: str) -> Tuple[Any, str]:
#         """ Thực thi câu lệnh trả về một giá trị duy nhất """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(query)
#                 self.conn.commit()
#                 result = cursor.fetchone()
#             return (result[0] if result else None)
#         except Exception as e:
#             return None, f"Error executing scalar query: {str(e)}"

#     def execute_s_procedure(self, sprocedure_name: str, *params: Any) -> str:
#         """ Gọi stored procedure không trả về dữ liệu """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(f"CALL {sprocedure_name}({', '.join(['%s'] * len(params))})", params)
#                 self.conn.commit()
#             return "Stored procedure executed successfully."
#         except Exception as e:
#             return f"Error executing stored procedure: {str(e)}"

#     def execute_scalar_s_procedure(self, sprocedure_name: str, *params: Any) -> Tuple[Any, str]:
#         """ Gọi stored procedure trả về một giá trị duy nhất """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(f"SELECT {sprocedure_name}({', '.join(['%s'] * len(params))})", params)
#                 self.conn.commit()
#                 result = cursor.fetchone()
#             return (result[0] if result else None)
#         except Exception as e:
#             return None, f"Error executing scalar stored procedure: {str(e)}"

#     def execute_s_procedure_return_data_table(self, func_name: str, *params: Any) -> Tuple[List[dict], str]:
#         """ Gọi stored procedure trả về bảng dữ liệu """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 cursor.execute(f"SELECT * FROM {func_name}({', '.join(['%s'] * len(params))})", params)
#                 self.conn.commit()
#                 return cursor.fetchall()
#         except Exception as e:
#             return [], f"Error executing stored procedure: {str(e)}"

#     def reader_s_procedure(self, func_name: str, *params: Any) -> Tuple[List[dict], str]:
#         """ Đọc dữ liệu từ stored procedure """
#         return self.execute_s_procedure_return_data_table(func_name, *params)
#---------------------------------------------------------------------------------------------------------------------------------------

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

    def execute_s_procedure(self, sprocedure_name: str, *params: Any, expected_types: Any) -> str:
        """ Gọi stored procedure không trả về dữ liệu """
        if not self.conn:
            self.open_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT {sprocedure_name}({', '.join([f'%s::{param_type}' for param_type in expected_types])})", params)
                # cursor.execute(f"CALL {sprocedure_name}({', '.join(['%s'] * len(params))})", params)
                self.conn.commit()
            return "Stored procedure executed successfully."
        except Exception as e:
            return f"Error executing stored procedure: {str(e)}"

    def execute_scalar_s_procedure(self, sprocedure_name: str, *params: Any, expected_types: Any) -> Tuple[Any, str]:
        """ Gọi stored procedure trả về một giá trị duy nhất """
        if not self.conn:
            self.open_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT {sprocedure_name}({', '.join([f'%s::{param_type}' for param_type in expected_types])})", params)
                # print((f"SELECT {sprocedure_name}({', '.join(['%s'] * len(params))})", params))
                # cursor.execute(f"SELECT {sprocedure_name}({', '.join(['%s'] * len(params))})", params)
                self.conn.commit()
                result = cursor.fetchone()
            return (result[0] if result else None, "")
        except Exception as e:
            return None, f"Error executing scalar stored procedure: {str(e)}"

    def execute_s_procedure_return_data_table(self, func_name: str, *params: Any, expected_types: Any) -> Tuple[List[dict], str]:
        """ Gọi stored procedure trả về bảng dữ liệu """
        if not self.conn:
            self.open_connection()
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT * FROM {func_name}({', '.join([f'%s::{param_type}' for param_type in expected_types])})", params)
                self.conn.commit()
                return cursor.fetchall(), ""
        except Exception as e:
            return [], f"Error executing stored procedure: {str(e)}"

    # def reader_s_procedure(self, func_name: str, *params: Any) -> Tuple[List[dict], str]:
    #     """ Đọc dữ liệu từ stored procedure """
    #     return self.execute_s_procedure_return_data_table(func_name, *params)
    
    @classmethod
    def close_pool(cls):
        """Đóng toàn bộ pool khi không cần nữa"""
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
