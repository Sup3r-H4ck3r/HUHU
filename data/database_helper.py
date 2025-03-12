# """Database helper module for PostgreSQL connection management and query execution."""
# import psycopg2
# import psycopg2.pool
# from psycopg2.extras import RealDictCursor
# from typing import Any, List, Tuple, Optional, Dict, Union, TypeVar

# T = TypeVar('T')
# QueryResult = Union[List[Dict[str, Any]], Any, None]


# class DatabaseHelper:
#     """Helper class for database operations with connection pooling support."""
    
#     _pool = None  # Class-level connection pool shared across all instances
    
#     @classmethod
#     def initialize_pool(cls, minconn: int, maxconn: int, connection_string: str) -> None:
#         """Initialize connection pool if not already created.
        
#         Args:
#             minconn: Minimum number of connections in the pool
#             maxconn: Maximum number of connections in the pool
#             connection_string: PostgreSQL connection string
#         """
#         if cls._pool is None:
#             cls._pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, connection_string)

#     def __init__(self, connection_string: str, use_pool: bool = True):
#         """Initialize DatabaseHelper.
        
#         Args:
#             connection_string: PostgreSQL connection string
#             use_pool: Whether to use connection pooling (True) or direct connections (False)
#         """
#         self.connection_string = connection_string
#         self.use_pool = use_pool
#         self.conn: Optional[psycopg2.extensions.connection] = None
    
#     def open_connection(self) -> str:
#         """Get a connection from the pool or open a new direct connection.
        
#         Returns:
#             Status message about the connection attempt
#         """
#         if self.conn:
#             return "Connection is already open."
        
#         try:
#             if self.use_pool:
#                 if not DatabaseHelper._pool:
#                     return "Connection pool is not initialized."
#                 self.conn = DatabaseHelper._pool.getconn()
#             else:
#                 self.conn = psycopg2.connect(self.connection_string)
#             return "Connection acquired successfully."
#         except Exception as e:
#             return f"Error acquiring connection: {str(e)}"
    
#     def get_procedure_param_types(self, procedure_name: str) -> List[str]:
#         """Get parameter types for a stored procedure.
        
#         Args:
#             procedure_name: Name of the stored procedure
            
#         Returns:
#             List of parameter types as strings
#         """
#         query = """
#         SELECT t.typname AS param_type
#         FROM pg_proc p
#         JOIN pg_namespace n ON p.pronamespace = n.oid
#         JOIN LATERAL unnest(proargnames, proargtypes::oid[]) 
#             WITH ORDINALITY AS param(param_name, type_oid, ordinality) 
#             ON true
#         JOIN pg_type t ON param.type_oid = t.oid
#         WHERE p.proname = %s
#         ORDER BY param.ordinality;
#         """
#         try:
#             if not self.conn:
#                 self.open_connection()
                
#             with self.conn.cursor() as cursor:
#                 cursor.execute(query, (procedure_name,))
#                 result = [i[0] for i in cursor.fetchall()]
#                 return result
#         except Exception:
#             return []

#     def close_connection(self) -> str:
#         """Return connection to pool or close direct connection.
        
#         Returns:
#             Status message about the connection closing attempt
#         """
#         if self.conn:
#             try:
#                 if self.use_pool:
#                     DatabaseHelper._pool.putconn(self.conn)
#                 else:
#                     self.conn.close()
#                 self.conn = None
#                 return "Connection closed successfully."
#             except Exception as e:
#                 return f"Error closing connection: {str(e)}"
#         return "No active connection to close."
    
#     def execute_non_query(self, query: str) -> str:
#         """Execute a query that doesn't return results (INSERT, UPDATE, DELETE).
        
#         Args:
#             query: SQL query to execute
            
#         Returns:
#             Status message about query execution
#         """
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
#         """Execute a query that returns a single value.
        
#         Args:
#             query: SQL query to execute
            
#         Returns:
#             Tuple containing (result, error_message)
#         """
#         if not self.conn:
#             self.open_connection()
#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.execute(query)
#                 self.conn.commit()
#                 result = cursor.fetchone()
#             return (result[0] if result else None, "")
#         except Exception as e:
#             return None, f"Error executing scalar query: {str(e)}"

#     def execute_stored_procedure(self, procedure_name: str, *params: Any) -> str:
#         """Execute a stored procedure with no result.
        
#         Args:
#             procedure_name: Name of the stored procedure
#             params: Parameters to pass to the stored procedure
            
#         Returns:
#             Status message about stored procedure execution
#         """
#         if not self.conn:
#             self.open_connection()
        
#         expected_types = self.get_procedure_param_types(procedure_name)
#         if not expected_types:
#             return f"Could not retrieve parameter types for {procedure_name}"
            
#         try:
#             with self.conn.cursor() as cursor:
#                 param_placeholders = [f'%s::{expected_types[i]}' for i in range(len(params))]
#                 cursor.execute(f"SELECT {procedure_name}({', '.join(param_placeholders)})", params)
#                 self.conn.commit()
#             return "Stored procedure executed successfully."
#         except Exception as e:
#             return f"Error executing stored procedure: {str(e)}"

#     def execute_scalar_stored_procedure(self, procedure_name: str, *params: Any) -> Tuple[Any, str]:
#         """Execute a stored procedure that returns a single value.
        
#         Args:
#             procedure_name: Name of the stored procedure
#             params: Parameters to pass to the stored procedure
            
#         Returns:
#             Tuple containing (result, error_message)
#         """
#         if not self.conn:
#             self.open_connection()
            
#         expected_types = self.get_procedure_param_types(procedure_name)
#         if not expected_types:
#             return None, f"Could not retrieve parameter types for {procedure_name}"
#         try:
#             with self.conn.cursor() as cursor:
#                 param_placeholders = [f'%s::{expected_types[i]}' for i in range(len(params))]
#                 cursor.execute(f"SELECT {procedure_name}({', '.join(param_placeholders)})", params)
#                 self.conn.commit()
#                 result = cursor.fetchone()
#             return (result[0] if result else None, "")
#         except Exception as e:
#             return None, f"Error executing scalar stored procedure: {str(e)}"

#     def execute_stored_procedure_return_data(
#         self, procedure_name: str, *params: Any
#     ) -> Tuple[List[Dict[str, Any]], str]:
#         """Execute a stored procedure that returns a table of data.
        
#         Args:
#             procedure_name: Name of the stored procedure
#             params: Parameters to pass to the stored procedure
            
#         Returns:
#             Tuple containing (result_set, error_message)
#         """
#         if not self.conn:
#             self.open_connection()
            
#         expected_types = self.get_procedure_param_types(procedure_name)
#         if not expected_types:
#             return [], f"Could not retrieve parameter types for {procedure_name}"
            
#         try:
#             with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 param_placeholders = [f'%s::{expected_types[i]}' for i in range(len(params))]
#                 cursor.execute(f"SELECT * FROM {procedure_name}({', '.join(param_placeholders)})", params)
#                 self.conn.commit()
#                 return cursor.fetchall(), ""
#         except Exception as e:
#             return [], f"Error executing stored procedure: {str(e)}"
    
#     @classmethod
#     def close_pool(cls) -> None:
#         """Close the entire connection pool."""
#         if cls._pool:
#             cls._pool.closeall()
#             cls._pool = None

"""Database helper module for PostgreSQL connection management and query execution using psycopg3."""
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from typing import Any, List, Tuple, Optional, Dict, Union, TypeVar

T = TypeVar('T')
QueryResult = Union[List[Dict[str, Any]], Any, None]


class DatabaseHelper:
    """Helper class for database operations with connection pooling support."""
    
    _pool = None
    
    @classmethod
    def initialize_pool(cls, minconn: int, maxconn: int, connection_string: str) -> None:
        """Initialize connection pool if not already created.
        
        Args:
            minconn: Minimum number of connections in the pool
            maxconn: Maximum number of connections in the pool
            connection_string: PostgreSQL connection string
        """
        if cls._pool is None:
            cls._pool = ConnectionPool(connection_string, min_size=minconn, max_size=maxconn)

    def __init__(self, connection_string: str, use_pool: bool = True):
        """Initialize DatabaseHelper.
        
        Args:
            connection_string: PostgreSQL connection string
            use_pool: Whether to use connection pooling (True) or direct connections (False)
        """
        self.connection_string = connection_string
        self.use_pool = use_pool
        self.conn: Optional[psycopg.Connection] = None
    
    def open_connection(self) -> str:
        """Get a connection from the pool or open a new direct connection.
        
        Returns:
            Status message about the connection attempt
        """
        if self.conn:
            return "Connection is already open."
        
        try:
            if self.use_pool:
                if not DatabaseHelper._pool:
                    return "Connection pool is not initialized."
                self.conn = DatabaseHelper._pool.getconn()
            else:
                self.conn = psycopg.connect(self.connection_string)
                print("hehe")
            return "Connection acquired successfully."
        except Exception as e:
            return f"Error acquiring connection: {str(e)}"
    
    def get_procedure_param_types(self, procedure_name: str) -> List[str]:
        """Get parameter types for a stored procedure.
        
        Args:
            procedure_name: Name of the stored procedure
            
        Returns:
            List of parameter types as strings
        """
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
            if not self.conn:
                self.open_connection()
                
            with self.conn.cursor() as cursor:
                cursor.execute(query, [procedure_name])
                result = [i[0] for i in cursor.fetchall()]
                return result
        except Exception:
            return []

    def close_connection(self) -> str:
        """Return connection to pool or close direct connection.
        
        Returns:
            Status message about the connection closing attempt
        """
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
        """Execute a query that doesn't return results (INSERT, UPDATE, DELETE).
        
        Args:
            query: SQL query to execute
            
        Returns:
            Status message about query execution
        """
        if not self.conn:
            self.open_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()
            return "Query executed successfully."
        except Exception as e:
            return f"Error executing query: {str(e)}"
        finally:
            if not self.use_pool:
                self.close_connection()

    def execute_scalar(self, query: str) -> Tuple[Any, str]:
        """Execute a query that returns a single value.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple containing (result, error_message)
        """
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
        finally:
            if not self.use_pool:
                self.close_connection()

    def execute_stored_procedure(self, procedure_name: str, *params: Any) -> str:
        """Execute a stored procedure with no result.
        
        Args:
            procedure_name: Name of the stored procedure
            params: Parameters to pass to the stored procedure
            
        Returns:
            Status message about stored procedure execution
        """
        expected_types = self.get_procedure_param_types(procedure_name)
        if not expected_types:
            return f"Could not retrieve parameter types for {procedure_name}"
        
        if not self.conn:
            self.open_connection()
            
        try:
            with self.conn.cursor() as cursor:

                param_placeholders = [f'%s::{expected_types[i]}' for i in range(len(params))]
                cursor.execute(f"SELECT {procedure_name}({', '.join(param_placeholders)})", params)
                self.conn.commit()
            return "Stored procedure executed successfully."
        except Exception as e:
            return f"Error executing stored procedure: {str(e)}"
        finally:
            if not self.use_pool:
                self.close_connection()

    def execute_scalar_stored_procedure(self, procedure_name: str, *params: Any) -> Tuple[Any, str]:
        """Execute a stored procedure that returns a single value.
        
        Args:
            procedure_name: Name of the stored procedure
            params: Parameters to pass to the stored procedure
            
        Returns:
            Tuple containing (result, error_message)
        """
        expected_types = self.get_procedure_param_types(procedure_name)
        if not expected_types:
            return None, f"Could not retrieve parameter types for {procedure_name}"
        
        if not self.conn:
            self.open_connection()
            
        try:
            with self.conn.cursor() as cursor:
                param_placeholders = [f'%s::{expected_types[i]}' for i in range(len(params))]
                cursor.execute(f"SELECT * FROM {procedure_name}({', '.join(param_placeholders)})", params)
                self.conn.commit()
                result = cursor.fetchone()
            return (result[0] if result else None, "")
        except Exception as e:
            return None, f"Error executing scalar stored procedure: {str(e)}"
        finally:
            if not self.use_pool:
                self.close_connection()

    def execute_stored_procedure_return_data(
        self, procedure_name: str, *params: Any
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Execute a stored procedure that returns a table of data.
        
        Args:
            procedure_name: Name of the stored procedure
            params: Parameters to pass to the stored procedure
            
        Returns:
            Tuple containing (result_set, error_message)
        """
        expected_types = self.get_procedure_param_types(procedure_name)
        if not expected_types:
            return [], f"Could not retrieve parameter types for {procedure_name}"
        
        if not self.conn:
            self.open_connection()
            
        try:
            with self.conn.cursor(row_factory=dict_row) as cursor:
                expected_types = ['integer', 'integer', 'char']
                param_placeholders = [f'%s::{expected_types[i]}' for i in range(len(params))]
                cursor.execute(f"SELECT * FROM {procedure_name}({', '.join(param_placeholders)})", params)
                self.conn.commit()
                return cursor.fetchall(), ""
        except Exception as e:
            return [], f"Error executing stored procedure: {str(e)}"
        finally:
            if not self.use_pool:
                self.close_connection()
    
    @classmethod
    def close_pool(cls) -> None:
        """Close the entire connection pool."""
        if cls._pool:
            cls._pool.close()
            cls._pool = None
