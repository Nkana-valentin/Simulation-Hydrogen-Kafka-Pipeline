from datetime import datetime
from typing import Optional, List, Union
import logging
import sys
#from apps.table_manager import TableManager
#import DataQueryBuilder

class DataQueryBuilder:
    """
    Simple query builder for hydrogen data
    """
    
    def __init__(self, 
                table_name: str):
        self.table_name = table_name
        self.default_columns = ["timestamp", 
                                "lab", 
                                "sensor_id", 
                                "measurement_type", 
                                "unit", 
                                "qualityflag", 
                                "value"
                                ]
    
    def _format_columns(self, 
                columns: Optional[Union[List[str], str]] = None) -> str:
        """
        Format columns for SELECT clause
        """
        if columns is None:
            columns = self.default_columns
        
        if isinstance(columns, list):
            return ",\n        ".join(columns)
        return columns  # string case
    
    def build_query(self, 
                    last_timestamp: Optional[str] = None, 
                    batch_size: int = 1000,
                    columns: Optional[Union[List[str], str]] = None) -> str:
        """
        Build query for hydrogen data
        Args:
            last_timestamp: Last synced timestamp (None for initial sync)
            batch_size: Number of records to fetch
            columns: Columns to select (list, string, or None for defaults)
        Returns:
            SQL query string
        """
        cols = self._format_columns(columns)
        
        if last_timestamp:
            # Incremental sync
            return f"SELECT {cols} FROM {self.table_name} WHERE timestamp > '{last_timestamp}' ORDER BY timestamp ASC LIMIT {batch_size}"
        else:
            # Initial sync
            return f"SELECT {cols} FROM {self.table_name} ORDER BY timestamp ASC LIMIT {batch_size}"