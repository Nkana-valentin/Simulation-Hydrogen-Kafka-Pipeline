"""
Data quality validation module for H2 laboratory data.
Validates JSON data before ingestion into QuestDB.
"""
import json
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple, Optional
from pydantic import BaseModel, validator, Field
from enum import Enum
import structlog

logger = structlog.get_logger()

class DataType(Enum):
    """
    Supported data types for validation
    """
    TEMPERATURE = "temperature"
    PRESSURE = "pressure"
    FLOW_RATE = "flow_rate"
    VOLTAGE = "voltage"
    CURRENT = "current"
    EFFICIENCY = "efficiency"
    CUSTOM = "custom"

class RangeRule(BaseModel):
    """
    Validation rule for numeric ranges
    """
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_deviation: Optional[float] = None  # Percentage

class QualityRule(BaseModel):
    """
    Quality validation rule for a specific measurement
    """
    field_name: str
    data_type: DataType
    required: bool = True
    range_rule: Optional[RangeRule] = None
    allowed_values: Optional[List[Any]] = None
    timestamp_format: str = "%Y-%m-%dT%H:%M:%S.%fZ"

class DataQualityChecker:
    """
    Main quality checker class
    """
    
    def __init__(self, config_path: str = "config/quality_rules.json"):
        self.rules = self._load_rules(config_path)
        self.validation_results = []
        
    def _load_rules(self, config_path: str) -> Dict[str, QualityRule]:
        """
        Load validation rules from configuration
        """
        # Default rules for common H2 lab measurements
        default_rules = {
            "temperature": QualityRule(
                field_name="temperature",
                data_type=DataType.TEMPERATURE,
                range_rule=RangeRule(min_value=-50, max_value=500)
            ),
            "pressure": QualityRule(
                field_name="pressure",
                data_type=DataType.PRESSURE,
                range_rule=RangeRule(min_value=0, max_value=1000)
            ),
            "flow_rate": QualityRule(
                field_name="flow_rate",
                data_type=DataType.FLOW_RATE,
                range_rule=RangeRule(min_value=0, max_value=100)
            ),
            "timestamp": QualityRule(
                field_name="timestamp",
                data_type=DataType.CUSTOM,
                required=True
            )
        }
        return default_rules
    
    def validate_single_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a single JSON record against quality rules
        Args:
            record: Single data record as dictionary    
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required timestamp
        if "timestamp" not in record:
            errors.append("Missing required field: timestamp")
        else:
            try:
                # Parse and validate timestamp
                dt = datetime.strptime(record["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
                # Ensure timestamp is not in the future
                if dt > datetime.now(timezone.utc):
                    errors.append(f"Timestamp in future: {record['timestamp']}")
            except ValueError:
                errors.append(f"Invalid timestamp format: {record['timestamp']}")
        
        # Check numeric fields against rules
        for field_name, rule in self.rules.items():
            if field_name in record and rule.range_rule:
                value = record[field_name]
                if isinstance(value, (int, float)):
                    if rule.range_rule.min_value is not None and value < rule.range_rule.min_value:
                        errors.append(f"{field_name} below minimum: {value} < {rule.range_rule.min_value}")
                    if rule.range_rule.max_value is not None and value > rule.range_rule.max_value:
                        errors.append(f"{field_name} above maximum: {value} > {rule.range_rule.max_value}")
                else:
                    errors.append(f"{field_name} is not numeric: {type(value)}")
        
        # Check for NaN or infinity
        for key, value in record.items():
            if isinstance(value, float):
                if pd.isna(value):
                    errors.append(f"{key} contains NaN")
                elif not np.isfinite(value):
                    errors.append(f"{key} contains infinite value")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def validate_batch(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of records
        
        Args:
            data: List of data records
            
        Returns:
            Dictionary with validation results
        """
        results = {
            "total_records": len(data),
            "valid_records": 0,
            "invalid_records": 0,
            "errors": [],
            "valid_data": [],
            "invalid_data": []
        }
        
        for idx, record in enumerate(data):
            is_valid, errors = self.validate_single_record(record)
            
            if is_valid:
                results["valid_records"] += 1
                results["valid_data"].append(record)
            else:
                results["invalid_records"] += 1
                results["invalid_data"].append({
                    "index": idx,
                    "record": record,
                    "errors": errors
                })
                results["errors"].extend(errors)
        
        # Calculate quality metrics
        results["quality_score"] = (
            results["valid_records"] / results["total_records"]
            if results["total_records"] > 0 else 0
        )
        
        # Log results
        logger.info(
            "batch_validation_complete",
            total=results["total_records"],
            valid=results["valid_records"],
            invalid=results["invalid_records"],
            quality_score=f"{results['quality_score']:.2%}"
        )
        
        return results
    
    def generate_quality_report(self, validation_results: Dict[str, Any]) -> str:
        """
        Generate a human-readable quality report
        """
        report = [
            "=" * 60,
            "DATA QUALITY VALIDATION REPORT",
            "=" * 60,
            f"Total Records: {validation_results['total_records']}",
            f"Valid Records: {validation_results['valid_records']}",
            f"Invalid Records: {validation_results['invalid_records']}",
            f"Quality Score: {validation_results['quality_score']:.2%}",
            ""
        ]
        
        if validation_results["invalid_records"] > 0:
            report.append("ERROR DETAILS:")
            report.append("-" * 40)
            for invalid in validation_results["invalid_data"][:10]:  # Show first 10
                report.append(f"Record {invalid['index']}:")
                for error in invalid['errors']:
                    report.append(f"  - {error}")
            if len(validation_results["invalid_data"]) > 10:
                report.append(f"... and {len(validation_results['invalid_data']) - 10} more errors")
        
        return "\n".join(report)

# Example usage
if __name__ == "__main__":
    # Test with sample data
    checker = DataQualityChecker()
    
    sample_data = [
        {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "temperature": 25.5,
            "pressure": 101.3,
            "flow_rate": 12.5
        },
        {
            "timestamp": "2024-01-15T10:31:00.000Z",
            "temperature": 450.0,  # Might be valid
            "pressure": -5.0,  # Invalid: negative pressure
            "flow_rate": 12.6
        }
    ]
    
    results = checker.validate_batch(sample_data)
    print(checker.generate_quality_report(results))