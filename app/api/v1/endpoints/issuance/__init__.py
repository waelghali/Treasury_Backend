from fastapi import APIRouter
router = APIRouter()
from app.api.v1.endpoints.issuance.base import *
from app.api.v1.endpoints.issuance.banks import *
from app.api.v1.endpoints.issuance.forms import *
from app.api.v1.endpoints.issuance.requests import *
from app.api.v1.endpoints.issuance.workflow import *
from app.api.v1.endpoints.issuance.post_issuance import *
from app.api.v1.endpoints.issuance.action_center import *
from app.api.v1.endpoints.issuance.reconciliation import *
from app.api.v1.endpoints.issuance.admin import *
from app.api.v1.endpoints.issuance.analytics import *
from app.api.v1.endpoints.issuance.migration import router as _migration_router
router.include_router(_migration_router)
