import requests
import logging
from typing import Dict
#from datetime import datetime, timezone
#from ingestion.json_tsdb_manager import Json2TsdbTransformer
import yaml
logger = logging.getLogger(__name__)


class QuestDBClient:
    """
    QuestDB client for table management
    """
    
    def __init__(self,  
                config_path: str = "TSDB.yml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.QUESTDB_QUERY_URL = f"http://{self.config['questdb']['host']}:{self.config['questdb']['port']}/exec"
        self.QUESTDB_WRITE_URL = f"http://{self.config['questdb']['host']}:{self.config['questdb']['port']}/write"
        logger.info(f"🔌 Connected to QuestDB at {self.config['questdb']['host']}:{self.config['questdb']['port']}")
        
    def _load_config(self) -> Dict:
        """
        Load configuration from YAML file
        """
        try:
            with open(self.config_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"questdb": {"host": "localhost", 
                                "port": self.config['questdb']['port']
                                }  
                    }    
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in QuestDB
        """
        try:
            # QuestDB provides a tables() function that returns all tables
            query = f"SELECT * FROM tables() WHERE table_name = '{table_name}'"
            
            response = requests.get(
                self.QUESTDB_QUERY_URL,
                params={"query": query},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                # Check if any rows were returned
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
    
    def insert_data(self, 
                    table_name: str, 
                    line_protocol: str) -> bool:
        """
        Insert data using InfluxDB line protocol
        """
        #print(f"\n Inserting data: {line_protocol}")  # Debug print
        try:
            # QuestDB accepts InfluxDB line protocol via /imp endpoint
            response = requests.post(
                self.QUESTDB_WRITE_URL,
                data=line_protocol,
                headers={'precision': "n"},
                timeout=10
            )
            #print(f"\n Insert response: {response.status_code} - {response.text}")  # Debug print
            if response.status_code in (200, 201, 204):
                logger.debug(f"✅ Data inserted into {table_name}")
                return True
            else:
                logger.error(f"❌ Insert failed: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Insert error: {e}")
            return False
        

# ## Usage example
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
    
#     # Create client
#     client = QuestDBClient()
    
#     # Define schema
#     schema = {
#         'tags': ['lab', 'sensor_id', 'measurement', 'unit'],
#         'fields': ['value']
#     }
    
#     #print(json2tsdb.tag_keys)
#     #print(json2tsdb.__dict__)
#     # Create table
#     if client.create_table("send_data_test", schema):
#         #print("Table created successfully")
#         json2tsdb = Json2TsdbTransformer(table_name="send_data_test", 
#                                             tag_keys=schema.get('tags'), 
#                                             field_keys=schema.get('fields'))
        
#         test_data = {
#             'auth': {'lab': 'chemistry_lab'},
#             'sensor_id': 'sensor_001',
#             'measurement': 'temperature',
#             'unit': 'celsius',
#             'value': 23.5,
#             'timestamp': '2024-01-01T12:00:00Z'
#         }
        
#         line_protocol = json2tsdb.transform(test_data)
#         #line_protocol = transform_to_influx_line(test_data, "insert_data_test")
#         #print(f"\n Line protocol: {line_protocol}")  # Debug print
#         client.insert_data("insert_data_test", line_protocol)
#     else:
#         print("Failed to create table")