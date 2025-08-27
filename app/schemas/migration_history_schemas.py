# app/schemas/migration_history_schemas.py

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any

class StagingSnapshotOut(BaseModel):
    id: int
    lg_number: str
    issuance_date: str
    expiry_date: str
    diff: Optional[Dict[str, Any]] = None
    conflicts: Optional[Dict[str, Any]] = None
    migration_timestamp: Optional[datetime] = None

class MigrationHistoryPreviewOut(BaseModel):
    lg_number: str
    snapshots: List[StagingSnapshotOut]
    conflict_flag: bool = False
    
class ImportHistoryIn(BaseModel):
    lg_numbers: Optional[List[str]] = Field(None, description="List of LG numbers to import. If empty, all READY_FOR_IMPORT records with history groups will be imported.")
    batch_note: Optional[str] = Field(None, description="Notes for the migration batch.")
    
class MigrationBatchOut(BaseModel):
    id: int
    started_at: datetime
    finished_at: Optional[datetime] = None
    user_id: int
    source_files: Optional[List[str]] = None
    totals: Optional[Dict[str, Any]] = None
    notes: Optional[str] = None
    
    class Config:
        from_attributes = True

class MigrationReportOut(BaseModel):
    total_staged_records: int
    summary_by_status: Dict[str, int]
    last_batch: Optional[MigrationBatchOut] = None