# app/schemas/migration_schemas.py

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum

class MigrationRecordStatusEnum(str, Enum):
    PENDING = "PENDING"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    READY_FOR_IMPORT = "READY_FOR_IMPORT"
    IMPORTED = "IMPORTED"
    REJECTED = "REJECTED"
    ERROR = "ERROR"
    DUPLICATE = "DUPLICATE"
    EXPIRED = "EXPIRED"

# NEW: Enum for migration record type
class MigrationTypeEnum(str, Enum):
    RECORD = "RECORD"
    INSTRUCTION = "INSTRUCTION"
    
class MigrationActionTypeEnum(str, Enum):
    LG_RECORD = "LG_RECORD"
    LG_INSTRUCTION = "LG_INSTRUCTION"    

class LGMigrationStagingIn(BaseModel):
    file_name: Optional[str] = Field(None, description="The name of the uploaded file.")
    source_data_json: Dict[str, Any] = Field(..., description="The raw, extracted data from the source document.")
    # NEW: Add field to specify if this is a full record or an instruction
    migration_type: MigrationTypeEnum = MigrationTypeEnum.RECORD

class LGMigrationStagingCreate(LGMigrationStagingIn):
    """Schema for creating a staging record, including status and logs."""
    record_status: MigrationRecordStatusEnum = MigrationRecordStatusEnum.PENDING
    validation_log: Optional[Dict[str, Any]] = None
    # NEW: Add migration_type to the create schema
    migration_type: MigrationTypeEnum = MigrationTypeEnum.RECORD


class LGMigrationStagingOut(BaseModel):
    id: int
    created_at: datetime
    file_name: Optional[str]
    record_status: MigrationRecordStatusEnum
    validation_log: Optional[Dict[str, Any]] = None
    internal_notes: Optional[str] = None
    source_data_json: Optional[Dict[str, Any]] = None
    structured_data_json: Optional[Dict[str, Any]] = None
    # NEW: Add migration_type to the output schema
    migration_type: MigrationTypeEnum

    class Config:
        from_attributes = True
        
class MigrationReportSummary(BaseModel):
    total_records: int
    pending_count: int
    needs_review_count: int
    ready_for_import_count: int
    imported_count: int
    rejected_count: int
    error_count: int
    duplicates: int
    expired_count: int 
    
class ProcessingSummaryOut(BaseModel):
    processed_count: int
    ready_for_import: List[int]
    duplicates: List[int]
    expired: List[int]
    errors: List[int]

class LGMigrationStagingUpdateIn(BaseModel):
    source_data_json: Dict[str, Any]
    
# NEW: Schema for the combined upload/processing response
class MigrationUploadResponse(BaseModel):
    message: str
    imported_count: int
    failed_count: int
    duplicate_count: int
    staged_records: List[LGMigrationStagingOut]
    
class RevalidateRecordsIn(BaseModel):
    ids: List[int]