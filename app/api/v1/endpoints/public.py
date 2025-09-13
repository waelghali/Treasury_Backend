# app/api/v1/endpoints/public.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
# 🐛 FIX: Import LegalArtifactOut from your schemas file
from app.schemas.all_schemas import LegalArtifactVersionsOut, LegalArtifactOut
from app.crud.crud import crud_legal_artifact, crud_global_configuration
from app.constants import GlobalConfigKey, LegalArtifactType

import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/legal-versions", response_model=LegalArtifactVersionsOut)
def get_legal_versions(
    db: Session = Depends(get_db)
):
    """
    Retrieves the latest version of the Terms and Conditions and Privacy Policy.
    This is a public endpoint and does not require authentication.
    """
    try:
        # Fetch versions from the LegalArtifact model
        latest_tc_artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.TERMS_AND_CONDITIONS)
        latest_pp_artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.PRIVACY_POLICY)

        # Fallback to GlobalConfiguration if LegalArtifact is not found
        if not latest_tc_artifact:
            tc_version_config = crud_global_configuration.get_by_key(db, GlobalConfigKey.TC_VERSION)
            tc_version = float(tc_version_config.value_default) if tc_version_config else 0.0
        else:
            tc_version = latest_tc_artifact.version

        if not latest_pp_artifact:
            pp_version_config = crud_global_configuration.get_by_key(db, GlobalConfigKey.PP_VERSION)
            pp_version = float(pp_version_config.value_default) if pp_version_config else 0.0
        else:
            pp_version = latest_pp_artifact.version
            
        logger.info(f"Retrieved legal artifact versions: TC={tc_version}, PP={pp_version}")
        return LegalArtifactVersionsOut(tc_version=tc_version, pp_version=pp_version)

    except Exception as e:
        logger.error(f"Failed to retrieve legal artifact versions: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching legal versions."
        )

@router.get("/legal-content/{artifact_type}", response_model=LegalArtifactOut)
def get_legal_content(
    artifact_type: str,
    db: Session = Depends(get_db)
):
    """
    Retrieves the latest content of a specific legal artifact.
    This is a public endpoint and does not require authentication.
    """
    # Validate artifact_type to prevent arbitrary file access
    if artifact_type not in [LegalArtifactType.TERMS_AND_CONDITIONS.value, LegalArtifactType.PRIVACY_POLICY.value]:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid legal artifact type.")

    try:
        artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type=artifact_type)
        if not artifact:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No content found for legal artifact type: {artifact_type}")
        return LegalArtifactOut.model_validate(artifact)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to retrieve legal artifact content for {artifact_type}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching legal content."
        )