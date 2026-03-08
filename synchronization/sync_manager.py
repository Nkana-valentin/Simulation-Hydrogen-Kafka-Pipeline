"""
Synchronization manager for ORFEO-SISSA data exchange.
Handles sync logic, state management, and job processing.
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import uuid
import json
from enum import Enum
import structlog

import redis
from celery import Celery
import pandas as pd

from src.database.questdb_client import QuestDBClient
from . import models
from config.settings import get_redis_config, get_questdb_config

logger = structlog.get_logger()

# Celery app for async task processing
celery_app = Celery(
    'sync_tasks',
    broker=get_redis_config()['url'],
    backend=get_redis_config()['url']
)

class SyncMode(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    LATEST = "latest"

class SyncStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"

class SyncManager:
    """
    Main synchronization manager class
    """
    
    def __init__(self):
        self.redis_client = redis.Redis.from_url(get_redis_config()['url'])
        self.db_client = QuestDBClient()
        self.sync_state_key = "sync:state:{dataset}"
        self.job_state_key = "sync:job:{job_id}"
        
    async def synchronize(
        self,dataset_name: str,
        mode: SyncMode = SyncMode.INCREMENTAL,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None, 
        fields: Optional[List[str]] = None,
        resolution: Optional[str] = None, 
        chunk_size: int = 10000) -> Dict[str, Any]:
        """
        Main synchronization method
        
        Args:
            dataset_name: Name of dataset to sync
            mode: Sync mode (full/incremental/latest)
            start_time: Start timestamp for data range
            end_time: End timestamp for data range
            fields: Specific fields to retrieve
            resolution: Downsampling resolution
            chunk_size: Records per chunk
            
        Returns:
            Dictionary with sync results
        """
        logger.info(
            "sync_started",
            dataset=dataset_name,
            mode=mode.value,
            start_time=start_time,
            end_time=end_time
        )
        
        try:
            # Get last sync state
            last_sync = self._get_last_sync_state(dataset_name)
            
            # Build query based on sync mode
            query, params = self._build_sync_query(
                dataset_name=dataset_name,mode=mode,last_sync=last_sync,
                start_time=start_time,
                end_time=end_time,
                fields=fields,resolution=resolution
            )
            
            # Execute query and fetch data
            data = await self.db_client.execute_query(query, params)
            
            # Apply chunking if needed
            chunks = self._chunk_data(data, chunk_size) if chunk_size else [data]
            
            # Update sync state
            self._update_sync_state(dataset_name, {
                "last_sync_time": datetime.utcnow().isoformat() + "Z",
                "records_synced": len(data),
                "mode": mode.value
            })
            
            result = {
                "dataset": dataset_name,
                "sync_mode": mode.value,
                "record_count": len(data),
                "data": data if len(chunks) == 1 else chunks[0],
                "time_range": f"{start_time or 'beginning'} to {end_time or 'now'}",
                "chunk_count": len(chunks) if len(chunks) > 1 else None,
                "chunk_info": {
                    "size": chunk_size,
                    "total": len(chunks)
                } if len(chunks) > 1 else None
            }
            
            if len(chunks) > 1:
                result["pagination"] = {
                    "current_chunk": 1,
                    "total_chunks": len(chunks),
                    "next_chunk": 2 if len(chunks) > 1 else None
                }
            
            logger.info(
                "sync_completed",
                dataset=dataset_name,
                records=len(data),
                chunks=len(chunks) if len(chunks) > 1 else 1
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "sync_failed",
                dataset=dataset_name,
                error=str(e)
            )
            raise
    
    def _build_sync_query(self,dataset_name: str,
        mode: SyncMode,
        last_sync: Optional[Dict],
        start_time: Optional[str],
        end_time: Optional[str],
        fields: Optional[List[str]],
        resolution: Optional[str]) -> Tuple[str, Dict[str, Any]]:
        """
        Build QuestDB query for synchronization
        """
        
        # Base query
        if fields:
            fields_str = ", ".join(fields)
        else:
            fields_str = "*"
        
        query = f"SELECT {fields_str} FROM {dataset_name}"
        params = {}
        
        # Add WHERE conditions based on mode
        conditions = []
        
        if mode == SyncMode.INCREMENTAL and last_sync:
            last_time = last_sync.get("last_sync_time")
            if last_time:
                conditions.append(f"timestamp > '{last_time}'")
        
        if start_time:
            conditions.append(f"timestamp >= '{start_time}'")
        
        if end_time:
            conditions.append(f"timestamp <= '{end_time}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Add SAMPLE BY for downsampling
        if resolution:
            query += f" SAMPLE BY {resolution}"
        
        # Order by timestamp
        query += " ORDER BY timestamp"
        
        return query, params
    
    def _chunk_data(self, data: List[Dict], 
                    chunk_size: int) -> List[List[Dict]]:
        """
        Split data into chunks for pagination
        """
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    def _get_last_sync_state(self, 
                            dataset_name: str) -> Optional[Dict]:
        """
        Get last sync state from Redis
        """
        key = self.sync_state_key.format(dataset=dataset_name)
        state_json = self.redis_client.get(key)
        return json.loads(state_json) if state_json else None
    
    def _update_sync_state(self, dataset_name: str, state: Dict):
        """
        Update sync state in Redis
        """
        key = self.sync_state_key.format(dataset=dataset_name)
        self.redis_client.setex(
            key,
            timedelta(days=30),  # Keep state for 30 days
            json.dumps(state)
        )
    
    async def validate_sissa_data(self, data: models.SISSADataPayload) -> Dict[str, Any]:
        """
        Validate data received from SISSA
        """
        errors = []
        
        # Check required fields
        if not data.data:
            errors.append("Empty data payload")
        
        # Validate timestamp format
        for idx, record in enumerate(data.data):
            if 'timestamp' not in record:
                errors.append(f"Record {idx}: Missing timestamp")
            else:
                try:
                    datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                except ValueError:
                    errors.append(f"Record {idx}: Invalid timestamp format")
        
        # Check for duplicate timestamps
        timestamps = [r.get('timestamp') for r in data.data]
        if len(timestamps) != len(set(timestamps)):
            errors.append("Duplicate timestamps found")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "records_checked": len(data.data)
        }
    
    async def process_sissa_data(self,job_id: str,
        dataset_name: str,
        data: models.SISSADataPayload,
        sissa_node_id: str):
        """
        Process data received from SISSA (async)
        """
        
        job_key = self.job_state_key.format(job_id=job_id)
        
        try:
            # Update job status
            self._update_job_status(job_id, 
                SyncStatus.RUNNING, 
                {"started_at": datetime.utcnow().isoformat() + "Z",
                "dataset": dataset_name,
                "records": len(data.data)
            })
            
            # Transform SISSA data format to ORFEO format
            transformed_data = self._transform_sissa_data(data.data)
            
            # Ingest to QuestDB
            from src.ingestion.json_to_tsdb import QuestDBIngestor
            ingestor = QuestDBIngestor()
            
            results = await ingestor.ingest_json_batch(
                transformed_data,
                validate=True)
            
            # Update job status
            self._update_job_status(job_id, 
                SyncStatus.COMPLETED, 
                {"completed_at": datetime.utcnow().isoformat() + "Z",
                "results": results,
                "sissa_node": sissa_node_id
            })
            
            logger.info(
                "sissa_data_processed",
                job_id=job_id,
                dataset=dataset_name,
                records_ingested=results["ingested_records"]
            )
            
        except Exception as e:
            logger.error(
                "sissa_data_processing_failed",
                job_id=job_id,
                error=str(e)
            )
            self._update_job_status(job_id, 
                SyncStatus.FAILED, 
                {"error": str(e),
                "failed_at": datetime.utcnow().isoformat() + "Z"
            })
    
    def _transform_sissa_data(self, data: List[Dict]) -> List[Dict]:
        """
        Transform SISSA data format to ORFEO format
        """
        transformed = []
        
        for record in data:
            transformed_record = {
                "timestamp": record.get("timestamp"),
                "source": "sissa_hydor",
                "data_type": record.get("type", "enriched"),
                "original_id": record.get("original_measurement_id"),
                "value": record.get("value"),
                "metadata": {
                    "confidence": record.get("confidence"),
                    "processing_method": record.get("processing_method"),
                    "digital_twin_link": record.get("digital_twin_link")
                }
            }
            
            # Add any additional fields
            for key, value in record.items():
                if key not in transformed_record:
                    transformed_record[key] = value
            
            transformed.append(transformed_record)
        
        return transformed
    
    def _update_job_status(self, job_id: str, 
                        status: SyncStatus, 
                        details: Dict = None):
        """
        Update job status in Redis
        """
        job_key = self.job_state_key.format(job_id=job_id)
        
        job_state = {
            "job_id": job_id,
            "status": status.value,
            "updated_at": datetime.utcnow().isoformat() + "Z"
        }
        
        if details:
            job_state.update(details)
        
        self.redis_client.setex(
            job_key,
            timedelta(days=7),  # Keep job state for 7 days
            json.dumps(job_state)
        )
    
    async def get_job_status(self, job_id: str) -> Dict:
        """
        Get status of a sync job
        """
        job_key = self.job_state_key.format(job_id=job_id)
        job_state_json = self.redis_client.get(job_key)
        
        if not job_state_json:
            raise KeyError(f"Job {job_id} not found")
        
        return json.loads(job_state_json)
    
    async def execute_sync_job(self, job_id: str, request: models.SyncJobRequest):
        """
        Execute a scheduled or manual sync job
        """
        self._update_job_status(job_id, SyncStatus.RUNNING, {
            "started_at": datetime.utcnow().isoformat() + "Z",
            "request": request.dict()
        })
        
        try:
            # Execute sync based on job type
            if request.job_type == "full_sync":
                result = await self.synchronize(
                    dataset_name=request.dataset_name,
                    mode=SyncMode.FULL,
                    start_time=request.start_time,
                    end_time=request.end_time
                )
            elif request.job_type == "incremental_sync":
                result = await self.synchronize(
                    dataset_name=request.dataset_name,
                    mode=SyncMode.INCREMENTAL,
                    start_time=request.start_time,
                    end_time=request.end_time
                )
            else:
                raise ValueError(f"Unknown job type: {request.job_type}")
            
            self._update_job_status(job_id, SyncStatus.COMPLETED, {
                "completed_at": datetime.utcnow().isoformat() + "Z",
                "result": result
            })
            
        except Exception as e:
            self._update_job_status(job_id, SyncStatus.FAILED, {
                "error": str(e),
                "failed_at": datetime.utcnow().isoformat() + "Z"
            })
            raise

# Celery tasks for async processing
@celery_app.task
def process_large_sync(dataset_name: str, mode: str, job_id: str):
    """
    Celery task for processing large sync jobs
    """
    manager = SyncManager()
    asyncio.run(manager.execute_sync_job(job_id, dataset_name, mode))

@celery_app.task
def process_sissa_ingestion(job_id: str, data: Dict, sissa_node_id: str):
    """
    Celery task for processing SISSA data ingestion
    """
    manager = SyncManager()
    
    # Convert dict back to Pydantic model
    payload = models.SISSADataPayload(**data)
    
    asyncio.run(manager.process_sissa_data(
        job_id=job_id,
        dataset_name="sissa_enriched_data",
        data=payload,
        sissa_node_id=sissa_node_id
    ))

# Dependency for FastAPI
async def get_sync_manager() -> SyncManager:
    return SyncManager()
