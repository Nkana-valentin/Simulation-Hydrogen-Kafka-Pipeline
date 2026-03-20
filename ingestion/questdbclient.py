import requests
import logging
from typing import Dict, Optional, List, Union
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)


class QuestDBClient:
    """
    QuestDB client for table management and data querying
    """
    
    def __init__(self,
                config_path: str = "TSDB.yml"):
        """
        Initialize QuestDB client with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.host = self.config['questdb']['host']
        self.port = self.config['questdb']['port']
        self.QUESTDB_QUERY_URL = f"http://{self.host}:{self.port}/exec"
        self.QUESTDB_WRITE_URL = f"http://{self.host}:{self.port}/write"
        self.table_name = self.config['kafka']['topics']['raw_data']
        self.create_table(self.table_name, {
            'tags': ['lab', 'sensor_id', 
                    'measurement_type', 
                    'unit', 
                    'qualityflag'],
            'fields': ['value']
        })
        
        # Default columns for queries
        self.default_columns = [
            "timestamp", 
            "lab", 
            "sensor_id", 
            "measurement_type", 
            "unit", 
            "qualityflag", 
            "value"
        ]
        
        logger.info(f"🔌 Connected to QuestDB at {self.host}:{self.port}")
    
    def _load_config(self) -> Dict:
        """
        Load configuration from YAML file
        """
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {self.config_path} not found, using defaults")
            return {"questdb": {"host": "localhost", "port": 9000}}
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"questdb": {"host": "localhost", "port": 9000}}
    
    # ==================== Table Management Methods ====================
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in QuestDB
        Args:
            table_name: Name of the table to check 
        Returns:
            True if table exists, False otherwise
        """
        try:
            query = f"SELECT * FROM tables() WHERE table_name = '{table_name}'"
            
            response = requests.get(
                self.QUESTDB_QUERY_URL,
                params={"query": query},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Table {table_name} exists and is accessible")
                result = response.json()
                return result.get('count', 0) > 0
            else:
                logger.error(f"❌ HTTP error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Error checking table existence: {e}")
            return False

    def create_table(self, table_name: str, schema: dict) -> bool:
        """
        Create table with proper schema in QuestDB (with existence check)
        Args:
            table_name: Name of the table to create
            schema: Dictionary with 'tags' and 'fields' lists  
        Returns:
            True if table created or already exists, False on error
        """
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"📊 Table {table_name} already exists, skipping creation")
                return True
            
            # Build column definitions
            columns = ["timestamp TIMESTAMP"]
            
            for tag in schema.get('tags', []):
                columns.append(f"{tag} STRING")
            
            for field in schema.get('fields', []):
                if field == 'value':
                    columns.append("value DOUBLE")
                else:
                    columns.append(f"{field} STRING")
            
            # QuestDB CREATE TABLE syntax
            create_query = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
            """
            
            logger.info(f"📊 Creating table: {table_name}")
            logger.debug(f"📝 Query: {create_query}")
            
            response = requests.get(
                self.QUESTDB_QUERY_URL,
                params={"query": create_query},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'error' in result:
                    logger.error(f"❌ QuestDB error: {result['error']}")
                    return False
                
                logger.info(f"✅ Table {table_name} created successfully")
                return True
            else:
                logger.error(f"❌ HTTP error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Unexpected error: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get the schema of an existing table
        Args:
            table_name: Name of the table   
        Returns:
            Dictionary with column names as keys and types as values, or None if table doesn't exist
        """
        try:
            if not self.check_table_exists(table_name):
                logger.warning(f"Table {table_name} does not exist")
                return None
            
            query = f"SELECT * FROM table_columns('{table_name}')"
            
            response = requests.get(
                self.QUESTDB_QUERY_URL,
                params={"query": query},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                schema = {}
                for row in result.get('dataset', []):
                    if len(row) >= 2:
                        schema[row[0]] = row[1]  # column_name: column_type
                return schema
            else:
                logger.error(f"❌ HTTP error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"💥 Error getting table schema: {e}")
            return None
    
    def drop_table(self, 
                    table_name: str, 
                    confirm: bool = False) -> bool:
        """
        Drop a table from QuestDB
        Args:
            table_name: Name of the table to drop
            confirm: Must be True to actually drop the table (safety measure) 
        Returns:
            True if table dropped successfully, False otherwise
        """
        if not confirm:
            logger.warning("⚠️ Table drop requires confirm=True")
            return False
        
        try:
            if not self.table_exists(table_name):
                logger.info(f"Table {table_name} does not exist, nothing to drop")
                return True
            
            query = f"DROP TABLE {table_name}"
            
            response = requests.get(
                self.QUESTDB_QUERY_URL,
                params={"query": query},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'error' in result:
                    logger.error(f"❌ QuestDB error: {result['error']}")
                    return False
                
                logger.info(f"✅ Table {table_name} dropped successfully")
                return True
            else:
                logger.error(f"❌ HTTP error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Error dropping table: {e}")
            return False
    
    # ==================== Data Insertion Methods ====================
    
    def insert_data(self, 
                    table_name: str, 
                    line_protocol: str) -> bool:
        """
        Insert data using InfluxDB line protocol
        Args:
            table_name: Name of the table to insert into
            line_protocol: Data in InfluxDB line protocol format  
        Returns:
            True if insert successful, False otherwise
        """
        try:
            response = requests.post(
                self.QUESTDB_WRITE_URL,
                data=line_protocol,
                headers={'precision': "n"},
                timeout=10
            )
            
            if response.status_code in (200, 201, 204):
                logger.debug(f"✅ Data inserted into {table_name}")
                return True
            else:
                logger.error(f"❌ Insert failed: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Insert error: {e}")
            return False
    
    def insert_batch(self, table_name: str, 
                    line_protocols: List[str]) -> Dict:
        """
        Insert multiple data points in batch
        Args:
            table_name: Name of the table to insert into
            line_protocols: List of line protocol strings
        Returns:
            Dictionary with success/failure counts
        """
        results = {
            'total': len(line_protocols),
            'success': 0,
            'failed': 0,
            'failures': []
        }
        
        # Join multiple lines with newlines for batch insert
        batch_data = "\n".join(line_protocols)
        
        try:
            response = requests.post(
                self.QUESTDB_WRITE_URL,
                data=batch_data,
                headers={'precision': "n"},
                timeout=30
            )
            
            if response.status_code in (200, 201, 204):
                results['success'] = len(line_protocols)
                logger.info(f"✅ Batch insert successful: {results['success']} records")
            else:
                # If batch fails, try individual inserts for better error tracking
                logger.warning("Batch insert failed, trying individual inserts")
                for i, lp in enumerate(line_protocols):
                    if self.insert_data(table_name, lp):
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                        results['failures'].append({'index': i, 'line_protocol': lp})
                        
        except Exception as e:
            logger.error(f"💥 Batch insert error: {e}")
            results['failed'] = len(line_protocols)
            
        return results
    
    # ==================== Query Building Methods ====================
    
    def _format_columns(self, columns: Optional[Union[List[str], str]] = None) -> str:
        """
        Format columns for SELECT clause
        Args:
            columns: List of columns, string, or None for defaults  
        Returns:
            Formatted column string for SQL query
        """
        if columns is None:
            columns = self.default_columns
        
        if isinstance(columns, list):
            return ",\n        ".join(columns)
        return columns  # string case
    
    def build_query(self, 
                    last_timestamp: Optional[str] = None, 
                    batch_size: int = 1000,
                    columns: Optional[Union[List[str], str]] = None,
                    where_clause: Optional[str] = None) -> str:
        """
        Build query for hydrogen data
        Args:
            table_name: Name of the table to query
            last_timestamp: Last synced timestamp (None for initial sync)
            batch_size: Number of records to fetch
            columns: Columns to select (list, string, or None for defaults)
            where_clause: Additional WHERE conditions (without the WHERE keyword)  
        Returns:
            SQL query string
        """
        cols = self._format_columns(columns)
        
        # Build WHERE clause
        where_parts = []
        if last_timestamp:
            where_parts.append(f"timestamp > '{last_timestamp}'")
        if where_clause:
            where_parts.append(where_clause)
        
        where_str = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        
        if last_timestamp or where_clause:
            # Incremental sync or filtered query
            return f"SELECT {cols} FROM {self.table_name} {where_str} ORDER BY timestamp ASC LIMIT {batch_size}"
        else:
            # Initial sync
            return f"SELECT {cols} FROM {self.table_name} ORDER BY timestamp ASC LIMIT {batch_size}"
    
    # ==================== Data Query Methods ====================
    
    def query(self, query: str) -> Optional[List[Dict]]:
        """
        Execute a raw SQL query and return results
        Args:
            query: SQL query string
        Returns:
            List of dictionaries with query results, or None on error
        """
        try:
            logger.debug(f"🔍 Executing query: {query}")
            
            response = requests.get(
                self.QUESTDB_QUERY_URL,
                params={"query": query},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if 'error' in result:
                    logger.error(f"❌ QuestDB error: {result['error']}")
                    return None
                
                # Convert to list of dictionaries
                columns = result.get('columns', [])
                dataset = result.get('dataset', [])
                
                records = []
                for row in dataset:
                    record = {}
                    for i, col in enumerate(columns):
                        if i < len(row):
                            record[col['name']] = row[i]
                    records.append(record)
                
                logger.debug(f"✅ Query returned {len(records)} records")
                return records
            else:
                logger.error(f"❌ HTTP error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"💥 Query error: {e}")
            return None
    
    def get_data(self, 
            table_name: str,
            last_timestamp: Optional[str] = None,
            batch_size: int = 1000,
            columns: Optional[Union[List[str], str]] = None,
            where_clause: Optional[str] = None) -> Optional[List[Dict]]:
        """
        Convenience method to build and execute a query
        
        Args:
            table_name: Name of the table to query
            last_timestamp: Last synced timestamp
            batch_size: Number of records to fetch
            columns: Columns to select
            where_clause: Additional WHERE conditions
            
        Returns:
            List of dictionaries with query results
        """
        query = self.build_query(
            table_name=table_name,
            last_timestamp=last_timestamp,
            batch_size=batch_size,
            columns=columns,
            where_clause=where_clause
        )
        
        return self.query(query)
    
    def get_latest_timestamp(self, table_name: str) -> Optional[str]:
        """
        Get the latest timestamp from a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Latest timestamp as string, or None if table is empty or doesn't exist
        """
        query = f"SELECT MAX(timestamp) as latest FROM {table_name}"
        results = self.query(query)
        
        if results and len(results) > 0 and results[0]['latest']:
            return results[0]['latest']
        
        return None
    
    def get_record_count(self, 
                    table_name: str, 
                    where_clause: Optional[str] = None) -> Optional[int]:
        """
        Get the number of records in a table
        Args:
            table_name: Name of the table
            where_clause: Optional WHERE condition
        Returns:
            Record count, or None on error
        """
        where_str = f"WHERE {where_clause}" if where_clause else ""
        query = f"SELECT COUNT(*) as count FROM {table_name} {where_str}"
        results = self.query(query)
        if results and len(results) > 0:
            return results[0]['count']
        
        return None


# # ==================== Usage Examples ====================

# def example_basic_usage():
#     """Basic usage examples"""
#     # Initialize client
#     client = QuestDBClient("TSDB.yml")
    
#     # Define schema
#     schema = {
#         'tags': ['lab', 'sensor_id', 'measurement_type', 'unit', 'qualityflag'],
#         'fields': ['value']
#     }
    
#     # Create table if it doesn't exist
#     table_name = "sensor_data"
#     if client.create_table(table_name, schema):
#         print(f"✅ Table {table_name} is ready")
    
#     # Insert some data (line protocol format)
#     line_protocol = "sensor_data,lab=chemistry_lab,sensor_id=sensor_001,measurement_type=temperature,unit=celsius,qualityflag=good value=23.5 1704110400000000000"
#     client.insert_data(table_name, line_protocol)
    
#     # Query the data
#     results = client.get_data(table_name, batch_size=10)
#     print(f"Query results: {results}")
    
#     # Get latest timestamp
#     latest = client.get_latest_timestamp(table_name)
#     print(f"Latest timestamp: {latest}")


# def example_incremental_sync():
#     """Example of incremental data sync"""
#     client = QuestDBClient()
#     table_name = "sensor_data"
    
#     # Get the latest timestamp we've synced
#     last_sync = client.get_latest_timestamp(table_name)
#     print(f"Last sync timestamp: {last_sync}")
    
#     # Get only new data since last sync
#     if last_sync:
#         new_data = client.get_data(
#             table_name=table_name,
#             last_timestamp=last_sync,
#             batch_size=1000
#         )
#         print(f"Found {len(new_data) if new_data else 0} new records")
#     else:
#         # Initial sync - get all data
#         all_data = client.get_data(table_name, batch_size=1000)
#         print(f"Initial sync: {len(all_data) if all_data else 0} records")


# def example_batch_insert():
#     """Example of batch data insertion"""
#     client = QuestDBClient()
#     table_name = "sensor_data"
    
#     # Create batch of line protocol strings
#     batch_data = []
#     base_time = 1704110400000000000
    
#     for i in range(5):
#         lp = f"sensor_data,lab=chemistry_lab,sensor_id=sensor_{i:03d},measurement_type=temperature,unit=celsius,qualityflag=good value={20.0 + i} {base_time + i*3600000000000}"
#         batch_data.append(lp)
    
#     # Insert batch
#     results = client.insert_batch(table_name, batch_data)
#     print(f"Batch insert results: {results}")


# def example_custom_query():
#     """Example with custom queries and filters"""
#     client = QuestDBClient()
#     table_name = "sensor_data"
    
#     # Get only temperature data from a specific lab
#     results = client.get_data(
#         table_name=table_name,
#         columns=['timestamp', 'sensor_id', 'value'],
#         where_clause="lab = 'chemistry_lab' AND measurement_type = 'temperature' AND value > 20.0",
#         batch_size=50
#     )
    
#     print(f"Temperature readings > 20°C: {results}")
    
#     # Get record count with filter
#     count = client.get_record_count(
#         table_name,
#         where_clause="measurement_type = 'temperature'"
#     )
#     print(f"Total temperature records: {count}")
    
#     # Raw SQL query
#     custom_query = f"""
#     SELECT 
#         lab,
#         measurement_type,
#         AVG(value) as avg_value,
#         COUNT(*) as reading_count
#     FROM {table_name}
#     WHERE timestamp > '2024-01-01'
#     GROUP BY lab, measurement_type
#     """
    
#     aggregated = client.query(custom_query)
#     print(f"Aggregated results: {aggregated}")


# def example_table_management():
#     """Example of table management operations"""
#     client = QuestDBClient()
    
#     # Check if table exists
#     if client.table_exists("sensor_data"):
#         print("Table exists")
        
#         # Get schema
#         schema = client.get_table_schema("sensor_data")
#         print(f"Table schema: {schema}")
        
#         # Get record count
#         count = client.get_record_count("sensor_data")
#         print(f"Total records: {count}")
    
#     # Drop table (with confirmation)
#     # client.drop_table("sensor_data", confirm=True)


# if __name__ == "__main__":
#     # Setup logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
    
#     print("=" * 50)
#     print("QuestDB Client Usage Examples")
#     print("=" * 50)
    
#     # Run examples
#     print("\n📌 Basic Usage Example:")
#     example_basic_usage()
    
#     print("\n📌 Batch Insert Example:")
#     example_batch_insert()
    
#     print("\n📌 Custom Query Example:")
#     example_custom_query()
    
#     print("\n📌 Table Management Example:")
#     example_table_management()
    
#     print("\n📌 Incremental Sync Example:")
#     example_incremental_sync()