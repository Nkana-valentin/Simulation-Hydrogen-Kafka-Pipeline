# ingestion/quality_check.py
"""
Data quality validation module for H2 laboratory data.
Validates JSON data before ingestion into QuestDB.
"""
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple, Optional
from pydantic import BaseModel
from enum import Enum
import structlog
import numpy as np

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
    
    
class DataQualityMetrics:
    """
    Implements advanced data quality metrics inspired by
    Peixoto et al. (2025) Data Quality Pipeline
    """

    def __init__(self, window_size: int = 60):
        self.window_size = window_size
        self.history = {}

    # ---------------------------
    # ACCURACY (Outlier detection)
    # ---------------------------
    def hampel_filter(self, 
                    values: List[float], 
                    threshold: float = 3):
        """
        Detect outliers using Hampel filter
        """
        if len(values) < 5:
            return []

        values = np.array(values)
        median = np.median(values)
        mad = np.median(np.abs(values - median))

        lower = median - threshold * mad
        upper = median + threshold * mad

        outliers = values[(values < lower) | (values > upper)]
        return outliers.tolist()

    def compute_accuracy_score(self, 
                            values: List[float]) -> float:
        """
        Accuracy metric based on outlier ratio
        """
        if len(values) == 0:
            return 0

        outliers = self.hampel_filter(values)
        accuracy = 1 - (len(outliers) / len(values))
        return max(0, accuracy)

    # ---------------------------
    # COMPLETENESS
    # ---------------------------
    def compute_content_completeness(self, 
                            record: Dict[str, Any]) -> float:
        """
        Completeness of fields inside record
        """
        total = len(record)
        missing = sum(v is None for v in record.values())

        return (total - missing) / total if total > 0 else 0

    def compute_temporal_completeness(self, 
                                    observed: int, 
                                    expected: int) -> float:
        """
        Temporal completeness (records per window)
        """
        if expected == 0:
            return 0
        return observed / expected

    # ---------------------------
    # CONSISTENCY
    # ---------------------------
    def compute_consistency(self, 
                            sensor_data: Dict[str, List[float]]) -> float:
        """
        Evaluate correlation consistency between sensors
        """
        sensors = list(sensor_data.keys())

        if len(sensors) < 2:
            return 1

        satisfied = 0
        total = 0

        for i in range(len(sensors)):
            for j in range(i + 1, len(sensors)):
                s1 = sensor_data[sensors[i]]
                s2 = sensor_data[sensors[j]]

                if len(s1) == len(s2) and len(s1) > 2:
                    corr = np.corrcoef(s1, s2)[0, 1]
                    total += 1

                    if corr > 0.8:
                        satisfied += 1

        return satisfied / total if total > 0 else 1

    # ---------------------------
    # TIMELINESS
    # ---------------------------
    def compute_timeliness(self, 
                    timestamp: datetime) -> float:
        """
        Compute timeliness score
        
        Args:
            timestamp: Can be timezone-aware or naive (assumed UTC if naive)
        """
        now = datetime.now(timezone.utc)
        
        # Ensure timestamp is timezone-aware
        if timestamp.tzinfo is None:
            # If naive, assume UTC
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        age = (now - timestamp).total_seconds()
        volatility = 120  # 2 minutes window
        
        score = max(0, 1 - age / volatility)
        return score

    # ---------------------------
    # WQS (Weighted Quality Score)
    # ---------------------------
    def compute_wqs(
        self,
        accuracy: float,
        completeness: float,
        wa: float = 0.7,
        wc: float = 0.3) -> float:
        """
        Weighted Quality Score
        """
        return wa * accuracy + wc * completeness

    # ---------------------------
    # LWQS (Historical Quality Score)
    # ---------------------------
    def compute_lwqs(self, 
                    history_scores: List[float], 
                    beta: int = 5) -> float:
        """
        Exponential decay weighted quality score
        """
        if not history_scores:
            return 0

        weights = []
        n = len(history_scores)

        for k in range(n):
            weight = np.exp(-(n - k - 1) / beta)
            weights.append(weight)

        weights = np.array(weights)
        weights = weights / weights.sum()

        return float(np.sum(np.array(history_scores) * weights))

    # ---------------------------
    # QSD (Quality Score Delta)
    # ---------------------------
    def compute_qsd(self, 
                    wqs: float, 
                    lwqs: float) -> float:
        """
        Quality Score Delta
        """
        return wqs - lwqs    

class DataQualityChecker:
    """
    Main quality checker class
    """
    
    def __init__(self, 
                config_path: str = "config/quality_rules.json"):
        self.rules = self._load_rules(config_path)
        self.validation_results = []
        
    def _load_rules(self, 
                config_path: str) -> Dict[str, QualityRule]:
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
    
    def _parse_timestamp(self, 
                    timestamp_str: str) -> datetime:
        """
        Parse timestamp string and ensure it's timezone-aware
        Args:
            timestamp_str: Timestamp string (e.g., "2024-01-15T10:30:00.000Z")   
        Returns:
            Timezone-aware datetime object
        """
        # Parse the timestamp
        dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Add UTC timezone
        return dt.replace(tzinfo=timezone.utc)
    
    def validate_single_record(self, 
                            record: Dict[str, Any]) -> Tuple[bool, List[str]]:
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
                # Parse and validate timestamp (makes it timezone-aware)
                dt = self._parse_timestamp(record["timestamp"])
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
    
    def generate_quality_report(self, 
                                validation_results: Dict[str, Any]) -> str:
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
    
    def compute_stream_quality(self, records: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Compute advanced quality metrics for streaming window
        """
        if not records:
            return {
                "accuracy": 0.0,
                "completeness": 0.0,
                "timeliness": 0.0,
                "wqs": 0.0
            }

        metrics = DataQualityMetrics()

        # Extract temperatures for accuracy calculation
        temperatures = [r["temperature"] for r in records if "temperature" in r]
        accuracy = metrics.compute_accuracy_score(temperatures) if temperatures else 0.0

        # Calculate completeness
        completeness_scores = [
            metrics.compute_content_completeness(r) for r in records
        ]
        completeness = np.mean(completeness_scores) if completeness_scores else 0.0

        # Parse timestamps with timezone awareness
        timestamps = []
        for r in records:
            if "timestamp" in r:
                try:
                    # Use the helper method to get timezone-aware datetime
                    ts = self._parse_timestamp(r["timestamp"])
                    timestamps.append(ts)
                except (ValueError, KeyError):
                    # Skip invalid timestamps
                    continue

        # Calculate timeliness with proper timezone handling
        timeliness = 0.0
        if timestamps:
            timeliness_scores = [
                metrics.compute_timeliness(ts) for ts in timestamps
            ]
            timeliness = np.mean(timeliness_scores)

        # Calculate WQS
        wqs = metrics.compute_wqs(accuracy, completeness)

        return {
            "accuracy": accuracy,
            "completeness": completeness,
            "timeliness": timeliness,
            "wqs": wqs
        }
        
    def print_stats(self):
        """
        Print current statistics
        """
        print("\n" + "-" * 50)
        print(f"📊 QUALITY STATS:")
        print(f"   📥 Total Received: {self.stats['total_received']}")
        print(f"   🔒 Auth Failed: {self.stats['auth_failed']}")
        print(f"   ⚙️  Processed: {self.stats['processed']}")
        print(f"   ├─ ✅ Valid: {self.stats['valid']}")
        print(f"   └─ ❌ Invalid: {self.stats['invalid']}")
        print(f"   📁 Quarantined: {self.stats['quarantined']}")
        
        if self.stats['total_received'] > 0:
            auth_success_rate = ((self.stats['total_received'] - self.stats['auth_failed']) / self.stats['total_received']) * 100
            valid_rate = (self.stats['valid'] / self.stats['total_received']) * 100
            print(f"\n   📈 Rates:")
            print(f"      Auth Success: {auth_success_rate:.1f}%")
            print(f"      Valid Data: {valid_rate:.1f}%")
            print(f"      Overall Yield: {valid_rate:.1f}%")
        print("-" * 50)    

# # Example usage
# if __name__ == "__main__":
#     # Test with sample data
#     checker = DataQualityChecker()
    
#     sample_data = [
#         {
#             "timestamp": "2024-01-15T10:30:00.000Z",
#             "temperature": 25.5,
#             "pressure": 101.3,
#             "flow_rate": 12.5
#         },
#         {
#             "timestamp": "2024-01-15T10:31:00.000Z",
#             "temperature": 450.0,  # Might be valid
#             "pressure": -5.0,  # Invalid: negative pressure
#             "flow_rate": 12.6
#         }
#     ]
    
#     results = checker.validate_batch(sample_data)
#     print(checker.generate_quality_report(results))