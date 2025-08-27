# app/crud/crud_migration_history.py

from typing import Any, Dict, List, Optional, Type
from sqlalchemy.orm import Session
from app.crud.base import CRUDBase, log_action
from app.models import MigrationBatch, LGChangeLog
from app.schemas.migration_history_schemas import MigrationBatchOut

class CRUDMigrationBatch(CRUDBase):
    def __init__(self, model: Type[MigrationBatch]):
        super().__init__(model)
        
    def get_batches(self, db: Session, skip: int = 0, limit: int = 100) -> List[MigrationBatch]:
        return db.query(self.model).order_by(self.model.started_at.desc()).offset(skip).limit(limit).all()

    def get_by_file_hash(self, db: Session, file_hash: str) -> Optional[MigrationBatch]:
        """
        Retrieves a migration batch by its file hash.
        This is used to prevent re-uploading the same file.
        """
        return db.query(self.model).filter(self.model.file_hash == file_hash).first()


        
class CRUDLGChangeLog(CRUDBase):
    def __init__(self, model: Type[LGChangeLog]):
        super().__init__(model)

    def create_change_log_entry(self, db: Session, lg_id: int, staging_id: Optional[int], change_index: int, diff_json: Dict[str, Any], note: Optional[str] = None) -> LGChangeLog:
        db_obj = self.model(
            lg_id=lg_id,
            staging_id=staging_id,
            change_index=change_index,
            diff_json=diff_json,
            note=note
        )
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        return db_obj