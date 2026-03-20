import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class Json2TsdbTransformer:
    """
    Transformer for converting JSON data to InfluxDB line protocol
    to send to the time-series database (e.g. QuestDB)
    """
    
    def __init__(self, 
            table_name: str = "default_table",
        tag_keys: Optional[List[str]] = None,
        field_keys: Optional[List[str]] = None):
        """
        Initialize the transformer with configuration
        Args:
            table_name: Name of the table to insert data into
            tag_keys: Default tags to extract
            field_keys: Default fields to extract
        """
        #self.measurement = measurement
        self.table_name = table_name
        self.tag_keys = tag_keys or ['lab', 'sensor_id', 'measurement', 'unit']
        self.field_keys = field_keys or ['value']
        
        #logger.info(f"🔧 Initialized transformer for measurement '{measurement}'")
        logger.debug(f"🏷️  Default tags: {self.tag_keys}")
        logger.debug(f"📊 Default fields: {self.field_keys}")
    
    def transform(self, 
        data: Dict[str, Any]=None, 
        tag_keys: Optional[List[str]] = None,
        field_keys: Optional[List[str]] = None) -> str:
        """
        Transform data to InfluxDB line protocol
        Args:
            data: Dictionary containing the data
            measurement: Override default measurement type
            tag_keys: Override default tag keys
            field_keys: Override default field keys   
        Returns:
            String in InfluxDB line protocol format
        """
        # Use instance defaults or overrides
        
        #meas = data.get('measurement') if data and 'measurement' in data else 'default_measurement'
        tags = tag_keys or self.tag_keys
        fields = field_keys or self.field_keys
        #print("================================")
        
        try:
            #logger.debug(f"🔄 Transforming to '{meas}'")
            auth = data.get('auth', {})
            
            # Parse timestamp
            timestamp_ns = self._parse_timestamp(data)
            
            # Build tags
            tag_string = self._build_tags(data, auth, tags)
            
            # Build fields
            field_string = self._build_fields(data, fields)
            
            # Construct line protocol
            line_protocol = f"{self.table_name},{tag_string} {field_string} {timestamp_ns}"
            #logger.info(f"✅ Successfully transformed {data} to {line_protocol}")
            logger.info(f"✅ Successfully transformed the data")
            #logger.debug(f"📝 Line protocol: {line_protocol}")
            return line_protocol
            
        except Exception as e:
            logger.error(f"💥 Transformation failed: {e}", exc_info=True)
            raise
    
    def _parse_timestamp(self, 
                        data: Dict[str, Any]) -> int:
        """
        Parse timestamp from data or use current time
        """
        if 'timestamp' in data:
            try:
                dt = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                return int(dt.timestamp() * 1_000_000_000)
            except (ValueError, AttributeError) as e:
                logger.warning(f"⚠️ Invalid timestamp format: {data.get('timestamp')}")
        
        timestamp_ns = int(time.time() * 1_000_000_000)
        logger.debug(f"⏱️  Using current timestamp: {timestamp_ns}")
        return timestamp_ns
    
    def _build_tags(self, 
                    data: Dict[str, Any], 
                    auth: Dict[str, Any], 
                    tag_keys: List[str]) -> str:
        """
        Build tags string from data
        """
        tag_parts = []
        
        for key in tag_keys:
            if key == 'lab':
                value = auth.get('lab', 'unknown')
            else:
                value = data.get(key, 'unknown')
            
            # Clean tag value
            if isinstance(value, str):
                value = value.replace(' ', '_').replace(',', '_')
            else:
                value = str(value)
            
            tag_parts.append(f"{key}={value}")
            
            if value == 'unknown':
                logger.debug(f"🏷️  Tag '{key}' set to 'unknown'")
        
        return ",".join(tag_parts)
    
    # def _build_fields(self, 
    #                 data: Dict[str, Any], 
    #                 field_keys: List[str]) -> str:
    #     """
    #     Build fields string from data
    #     """
    #     field_parts = []
        
    #     for key in field_keys:
    #         try:
    #             if key in data:
    #                 value = float(data[key])
    #                 field_parts.append(f"{key}={value}")
    #             else:
    #                 logger.debug(f"📊 Field '{key}' missing, using 0")
    #                 field_parts.append(f"{key}=0")
    #         except (ValueError, TypeError):
    #             logger.warning(f"📊 Invalid value for field '{key}': {data.get(key)}")
    #             field_parts.append(f"{key}=0")
        
    #     return " ".join(field_parts)
    
    
    def _build_fields(self, 
                data: Dict[str, Any], 
                field_keys: List[str]) -> str:
        """
        Build fields string from data
        """
        field_parts = []
        
        for key in field_keys:
            try:
                if key in data:
                    value = data[key]
                    # Handle different value types
                    if isinstance(value, (int, float)):
                        field_parts.append(f"{key}={value}")
                    elif isinstance(value, str):
                        # Escape quotes in string values
                        value = value.replace('"', '\\"')
                        field_parts.append(f"{key}=\"{value}\"")
                    elif isinstance(value, bool):
                        field_parts.append(f"{key}={str(value).lower()}")
                    else:
                        # Default to string for other types
                        field_parts.append(f"{key}=\"{value}\"")
                else:
                    logger.debug(f"📊 Field '{key}' missing, using 0")
                    field_parts.append(f"{key}=0")
            except (ValueError, TypeError) as e:
                logger.warning(f"📊 Invalid value for field '{key}': {data.get(key)}")
                field_parts.append(f"{key}=0")
        
        return " ".join(field_parts)
    
    def transform_batch(self, 
                        data_list: List[Dict[str, Any]], 
                        **kwargs) -> List[str]:
        """
        Transform a batch of data points
        """
        results = []
        for i, data in enumerate(data_list):
            try:
                results.append(self.transform(data, **kwargs))
            except Exception as e:
                logger.error(f"❌ Failed to transform item {i}: {e}")
                continue
        
        logger.info(f"📦 Batch transformed: {len(results)}/{len(data_list)} successful")
        return results

# Usage examples
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
    
#     # Create transformer with default settings
#     transformer = Json2TsdbTransformer()
    
#     test_data = {
#         'auth': {'lab': 'chemistry_lab'},
#         'sensor_id': 'sensor_001',
#         'measurement': 'temperature',
#         'unit': 'celsius',
#         'value': 23.5,
#         'timestamp': '2024-01-01T12:00:00Z'
#     }
    
#     # Use default measurement
#     result = transformer.transform(test_data)
#     print(f"\nDefault: {result}")
    
#     # Override measurement name
#     result = transformer.transform(test_data)
#     print(f"\n Overridden: {result}")
    
#     # Create transformer with different defaults
#     env_transformer = Json2TsdbTransformer(
#         tag_keys=['lab', 'sensor_id', 'location'],
#         field_keys=['temperature', 'humidity', 'pressure']
#     )
    
#     env_data = {
#         'auth': {'lab': 'physics_lab'},
#         'sensor_id': 'sensor_002',
#         'location': 'room_101',
#         'temperature': 22.5,
#         'humidity': 45.2,
#         'pressure': 1013.25,
#         'timestamp': '2024-01-01T12:00:00Z'
#     }
    
#     result = env_transformer.transform(env_data)
#     print(f"\nEnvironment: {result}")
    
#     # Batch processing
#     batch_data = [test_data, test_data, test_data]
#     results = transformer.transform_batch(batch_data)
#     print(f"\nBatch processed {len(results)} items")