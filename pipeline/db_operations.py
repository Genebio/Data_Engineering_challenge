"""Database operations module for the Hansel Attribution Pipeline."""

import sqlite3
from contextlib import contextmanager
from typing import Optional, Tuple

import pandas as pd


class DatabaseManager:
    """Database manager for SQLite operations."""
    
    def __init__(self, db_name: str):
        """Initialize DatabaseManager.
        
        Args:
            db_name: SQLite database file path
        """
        self.db_name = db_name
    
    @contextmanager
    def connection(self):
        """Context manager for database connections.
        
        Yields:
            sqlite3.Connection: Database connection
        """
        conn = sqlite3.connect(self.db_name)
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> None:
        """Execute a SQL query without returning results.
        
        Args:
            query: SQL query to execute
            params: Query parameters
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
    
    def read_sql(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            pd.DataFrame: Query results
        """
        with self.connection() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                         if_exists: str = 'append') -> None:
        """Insert DataFrame into a table.
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            if_exists: How to behave if the table exists
        """
        with self.connection() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)