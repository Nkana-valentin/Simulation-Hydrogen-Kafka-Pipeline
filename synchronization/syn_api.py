"""
FastAPI application for ORFEO-SISSA synchronization.
Provides GET/PUT endpoints for data exchange.
"""
from fastapi import FastAPI, HTTPException, Depends, Header, Query, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import uuid
import structlog

from . import models
from .sync_manager import SyncManager, get_sync_manager
from .security import SecurityMiddleware, rate_limit
from src.database.questdb_client import QuestDBClient
from src.utils.monitoring import monitor_request, metrics

logger = structlog.get_logger()
security = HTTPBearer()

app = FastAPI(
    title="ORFEO-SISSA Synchronization API",
    description="API for synchronizing H2 laboratory data between ORFEO and SISSA Hydor",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://sissa-hydor.it", "https://api.sissa-hydor.it"],
    allow_credentials=True,
    allow_methods=["GET", "PUT", "POST"],
    allow_headers=["*"],
)

# Security middleware
security_middleware = SecurityMiddleware()

@app.middleware("http")
async def add_security_middleware(request, call_next):
    return await security_middleware(request, call_next)

# Dependency injections
def get_db_client():
    return QuestDBClient()

def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verify API key from SISSA
    """
    api_key = credentials.credentials
    if not security_middleware.validate_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

@app.get("/")
@monitor_request
async def root():
    """
    API root endpoint
    """
    return {
        "service": "ORFEO-SISSA Synchronization API",
        "version": "1.0.0",
        "status": "operational"
    }

@app.get("/health")
async def health_check(db: QuestDBClient = Depends(get_db_client)):
    """
    Health check endpoint
    """
    try:
        # Check database connectivity
        db_status = await db.check_connection()
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "database": "connected" if db_status else "disconnected",
            "version": "1.0.0"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.get("/datasets")
@monitor_request
@rate_limit(requests_per_minute=30)
async def list_datasets(api_key: str = Depends(verify_api_key),
                        db: QuestDBClient = Depends(get_db_client)):
    """
    List available datasets for synchronization
    """
    try:
        datasets = await db.list_tables()
        
        return {
            "datasets": [
                {
                    "name": dataset["name"],
                    "type": dataset.get("type", "measurement"),
                    "record_count": dataset.get("count", 0),
                    "time_range": dataset.get("time_range"),
                    "last_updated": dataset.get("last_updated")
                }
                for dataset in datasets
            ],
            "count": len(datasets)
        }
    except Exception as e:
        logger.error("list_datasets_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to list datasets")

@app.get("/datasets/{dataset_name}/meta")
@monitor_request
async def get_dataset_metadata(
    dataset_name: str,
    api_key: str = Depends(verify_api_key),
    db: QuestDBClient = Depends(get_db_client)
):
    """
    Get metadata for a specific dataset
    """
    try:
        metadata = await db.get_table_metadata(dataset_name)
        
        if not metadata:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")
        
        return {
            "dataset": dataset_name,
            "metadata": metadata,
            "schema": metadata.get("columns", []),
            "partition_info": metadata.get("partition_by"),
            "indexes": metadata.get("indexes", [])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_metadata_failed", dataset=dataset_name, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get metadata")

@app.get("/datasets/{dataset_name}/data")
@monitor_request
@rate_limit(requests_per_minute=60)
async def get_dataset_data(
    dataset_name: str,
    start_time: Optional[str] = Query(None, description="Start time (ISO 8601)"),
    end_time: Optional[str] = Query(None, description="End time (ISO 8601)"),
    mode: str = Query("incremental", regex="^(full|incremental|latest)$"),
    format: str = Query("json", regex="^(json|csv|parquet)$"),
    chunk_size: int = Query(10000, ge=1, le=100000),
    fields: Optional[List[str]] = Query(None),
    resolution: Optional[str] = Query(None, regex="^(1s|1m|1h|1d)$"),
    x_sissa_node_id: Optional[str] = Header(None),
    x_request_id: Optional[str] = Header(None),
    api_key: str = Depends(verify_api_key),
    sync_manager: SyncManager = Depends(get_sync_manager)):
    """
    GET endpoint for SISSA to retrieve data from ORFEO
    Supports incremental sync, field selection, and data downsampling
    """
    request_id = x_request_id or str(uuid.uuid4())
    
    try:
        logger.info(
            "get_data_request",
            request_id=request_id,
            dataset=dataset_name,
            mode=mode,
            sissa_node=x_sissa_node_id
        )
        
        # Get sync data based on mode
        sync_result = await sync_manager.synchronize(
            dataset_name=dataset_name,
            mode=mode,
            start_time=start_time,
            end_time=end_time,
            fields=fields,
            resolution=resolution,
            chunk_size=chunk_size
        )
        
        # Format response based on requested format
        if format == "csv":
            # Convert to CSV
            import io
            output = io.StringIO()
            pd.DataFrame(sync_result["data"]).to_csv(output, index=False)
            return JSONResponse(
                content={"data": output.getvalue()},
                media_type="text/csv",
                headers={
                    "X-Request-ID": request_id,
                    "X-Record-Count": str(sync_result["record_count"])
                }
            )
        elif format == "parquet":
            # Convert to Parquet (in real implementation)
            raise HTTPException(status_code=501, detail="Parquet format not yet implemented")
        else:
            # JSON response (default)
            return {
                "metadata": {
                    "dataset": dataset_name,
                    "request_id": request_id,
                    "sync_mode": mode,
                    "time_range": sync_result.get("time_range"),
                    "record_count": sync_result["record_count"],
                    "chunk_info": sync_result.get("chunk_info"),
                    "schema_version": "1.0"
                },
                "pagination": sync_result.get("pagination", {}),
                "data": sync_result["data"]
            }
            
    except Exception as e:
        logger.error(
            "get_data_failed",
            request_id=request_id,
            dataset=dataset_name,
            error=str(e)
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve data: {str(e)}")

@app.put("/datasets/{dataset_name}/data")
@monitor_request
@rate_limit(requests_per_minute=20)
async def put_dataset_data(
    dataset_name: str,
    data: models.SISSADataPayload,
    background_tasks: BackgroundTasks,
    x_sissa_node_id: str = Header(...),
    x_request_id: Optional[str] = Header(None),
    api_key: str = Depends(verify_api_key),
    sync_manager: SyncManager = Depends(get_sync_manager)):
    """
    PUT endpoint for SISSA to send enriched/corrected data back to ORFEO
    Supports asynchronous processing of large datasets
    """
    request_id = x_request_id or str(uuid.uuid4())
    
    try:
        logger.info(
            "put_data_request",
            request_id=request_id,
            dataset=dataset_name,
            records=len(data.data),
            sissa_node=x_sissa_node_id,
            data_type=data.metadata.type
        )
        
        # Validate the incoming data
        validation_result = await sync_manager.validate_sissa_data(data)
        
        if not validation_result["valid"]:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Data validation failed",
                    "errors": validation_result["errors"]
                }
            )
        
        # Queue for asynchronous processing
        job_id = str(uuid.uuid4())
        background_tasks.add_task(
            sync_manager.process_sissa_data,
            job_id=job_id,
            dataset_name=dataset_name,
            data=data,
            sissa_node_id=x_sissa_node_id
        )
        
        return {
            "status": "accepted",
            "message": "Data queued for processing",
            "job_id": job_id,
            "request_id": request_id,
            "records_received": len(data.data),
            "estimated_processing_time": "30s",
            "check_status_url": f"/sync-jobs/{job_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "put_data_failed",
            request_id=request_id,
            dataset=dataset_name,
            error=str(e)
        )
        raise HTTPException(status_code=500, detail=f"Failed to process data: {str(e)}")

@app.post("/sync-jobs")
@monitor_request
async def create_sync_job(
    request: models.SyncJobRequest,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key),
    sync_manager: SyncManager = Depends(get_sync_manager)
):
    """
    Create a new synchronization job (for large or scheduled syncs)
    """
    job_id = str(uuid.uuid4())
    
    try:
        # Create async job
        background_tasks.add_task(
            sync_manager.execute_sync_job,
            job_id=job_id,
            request=request
        )
        
        return {
            "job_id": job_id,
            "status": "created",
            "message": f"Sync job created for {request.dataset_name}",
            "estimated_completion": (
                datetime.utcnow() + timedelta(minutes=5)
            ).isoformat() + "Z",
            "monitor_url": f"/sync-jobs/{job_id}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create sync job: {str(e)}")

@app.get("/sync-jobs/{job_id}")
@monitor_request
async def get_sync_job_status(
    job_id: str,
    api_key: str = Depends(verify_api_key),
    sync_manager: SyncManager = Depends(get_sync_manager)
):
    """Get status of a synchronization job"""
    try:
        status = await sync_manager.get_job_status(job_id)
        return status
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}")

@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    from prometheus_client import generate_latest
    return Response(
        generate_latest(metrics),
        media_type="text/plain"
    )

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "path": request.url.path,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error("unhandled_exception", error=str(exc), path=request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "path": request.url.path,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
