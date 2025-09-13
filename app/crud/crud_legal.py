# app/crud/crud_legal.py
from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from app.crud.base import CRUDBase
from app.models import LegalArtifact, UserLegalAcceptance, User
from app.schemas.all_schemas import LegalArtifactCreate
from app.constants import LegalArtifactType, GlobalConfigKey

# =====================================================================================
# CRUD for LegalArtifact and UserLegalAcceptance
# =====================================================================================
class CRUDLegalArtifact(CRUDBase):
    def get_by_artifact_type(self, db: Session, artifact_type: str) -> Optional[LegalArtifact]:
        """
        Retrieves the latest non-deleted legal artifact by its type.
        """
        return db.query(self.model).filter(
            self.model.artifact_type == artifact_type,
            self.model.is_deleted == False
        ).order_by(self.model.version.desc()).first()

    def create_artifact(self, db: Session, obj_in: LegalArtifactCreate) -> LegalArtifact:
        """
        Creates a new legal artifact. Any previous versions of the same artifact type
        are not automatically marked as deleted to preserve historical versions.
        """
        db_obj = self.model(**obj_in.model_dump())
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        return db_obj

class CRUDUserLegalAcceptance(CRUDBase):
    # NEW METHOD
    def has_accepted(self, db: Session, user_id: int, artifact_id: int) -> bool:
        """
        Checks if a user has already accepted a specific legal artifact version.
        """
        return db.query(self.model).filter(
            self.model.user_id == user_id,
            self.model.artifact_id == artifact_id
        ).first() is not None

    def record_acceptance(self, db: Session, user_id: int, artifact_id: int, ip_address: Optional[str]) -> UserLegalAcceptance:
        """
        Records a user's acceptance of a legal artifact.
        """
        db_obj = self.model(
            user_id=user_id,
            artifact_id=artifact_id,
            ip_address=ip_address,
            accepted_at=func.now()
        )
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        return db_obj

# =====================================================================================
# Centralized instances for easy import
# =====================================================================================
crud_legal_artifact = CRUDLegalArtifact(LegalArtifact)
crud_user_legal_acceptance = CRUDUserLegalAcceptance(UserLegalAcceptance)