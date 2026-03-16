# apps/table_manager.py
"""
Table management utilities for QuestDB
"""
import requests
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class TableManager:
    def __init__(self, questdb_query_url: str):
        self.questdb_query_url = questdb_query_url
    
    def list_tables(self) -> List[str]:
        """
        List all tables in QuestDB
        """
        try:
            response = requests.get(
                self.questdb_query_url,
                params={"query": "SELECT table_name FROM tables()"}
            )
            
            if response.status_code == 200:
                data = response.json()
                tables = [row[0] for row in data.get('dataset', [])]
                return tables
            else:
                logger.error(f"Failed to list tables: {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            return []
    
    def get_table_schema(self, table_name: str) -> Optional[List[Dict]]:
        """
        Get schema information for a table
        """
        try:
            response = requests.get(
                self.questdb_query_url,
                params={"query": f"SELECT * FROM {table_name} LIMIT 0"}
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('columns', [])
            else:
                logger.error(f"Failed to get schema for {table_name}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting schema: {str(e)}")
            return None
    
    def create_table(self, 
                    table_name: str, 
                    schema: Dict[str, str], 
                    timestamp_column: str = "timestamp") -> bool:
        """
        Create a new table
        """
        try:
            columns = [f"{col} {dtype}" for col, dtype in schema.items()]
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
            
            if timestamp_column in schema:
                create_query += f" timestamp({timestamp_column})"
            
            logger.info(f"Creating table: {table_name}")
            response = requests.get(
                self.questdb_query_url,
                params={"query": create_query}
            )
            
            return response.status_code in (200, 201, 204)
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            return False
    
    def initialize_required_tables(self, tables_config: Dict[str, Dict[str, str]]) -> Dict[str, bool]:
        """
        Initialize all required tables
        """
        results = {}
        
        for table_name, schema in tables_config.items():
            if self.check_table_exists(table_name):
                logger.info(f"Table '{table_name}' already exists")
                results[table_name] = True
            else:
                logger.info(f"Creating table '{table_name}'...")
                results[table_name] = self.create_table(table_name, schema)
        
        return results