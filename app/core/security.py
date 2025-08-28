# core/security.py
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import Depends, HTTPException, status, Request, Response
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, Field
from app.constants import UserRole, SubscriptionStatus
from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.database import get_db
from app.crud.crud import crud_user, crud_role_permission
from app.models import User, RolePermission

# NEW: Import hashing for password verification
from app.core.hashing import get_password_hash, verify_password_direct

# Environment variables for JWT
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
# MODIFIED: Increase expiration time for the frontend timer
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))

if SECRET_KEY is None:
    raise ValueError("SECRET_KEY environment variable is not set.")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v2/login", auto_error=False)

class TokenData(BaseModel):
    email: Optional[EmailStr] = None
    user_id: Optional[int] = None
    role: Optional[UserRole] = None
    permissions: List[str] = Field([], description="List of permission names associated with the user's role.")
    customer_id: Optional[int] = Field(None, description="Include customer_id in token data")
    has_all_entity_access: Optional[bool] = Field(True, description="True if user has access to all entities under their customer, False if restricted to specific entities")
    entity_ids: List[int] = Field([], description="List of customer entity IDs this user has access to.")
    must_change_password: Optional[bool] = Field(False, description="True if user must change password on next login.")
    subscription_status: Optional[SubscriptionStatus] = Field(None, description="Current subscription status of the customer.")


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# MODIFIED: get_current_user to check query params if header token is missing and to fetch subscription status
async def get_current_user(
    request: Request,
    response: Response,
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if token is None:
        token = request.query_params.get("token")
        if token is None:
            raise credentials_exception

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        user_id: int = payload.get("user_id")
        role: str = payload.get("role")
        customer_id: Optional[int] = payload.get("customer_id")
        has_all_entity_access: Optional[bool] = payload.get("has_all_entity_access")
        entity_ids: List[int] = payload.get("entity_ids", [])
        must_change_password: Optional[bool] = payload.get("must_change_password")
        subscription_status: Optional[str] = payload.get("subscription_status")

        if email is None or user_id is None or role is None:
            raise credentials_exception
        
        db_permissions = crud_role_permission.get_permissions_for_role(db, role)
        permission_names = [p.name for p in db_permissions]
        
        if subscription_status:
             subscription_status = SubscriptionStatus(subscription_status)

        token_data = TokenData(
            email=email,
            user_id=user_id,
            role=UserRole(role),
            permissions=permission_names,
            customer_id=customer_id,
            has_all_entity_access=has_all_entity_access,
            entity_ids=entity_ids,
            must_change_password=must_change_password,
            subscription_status=subscription_status
        )
    except JWTError:
        raise credentials_exception
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload or subscription status.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = crud_user.get(db, user_id)
    if user is None or user.is_deleted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is inactive or deleted.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # REMOVED: Sliding Expiration Logic
    # The frontend will now manage the inactivity timer.

    return token_data

async def get_current_active_user(current_user: TokenData = Depends(get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    
    if current_user.role != UserRole.SYSTEM_OWNER and current_user.must_change_password:
         raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must change your password on first login."
        )
    return current_user

# NEW DEPENDENCY: Check subscription status from token
def check_subscription_status(
    current_user: TokenData = Depends(get_current_active_user)
):
    if current_user.role == UserRole.SYSTEM_OWNER:
        return current_user
        
    if current_user.subscription_status == SubscriptionStatus.EXPIRED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Subscription is expired. Access is denied. Please contact the system owner to renew."
        )
    return current_user

# NEW DEPENDENCY: Check for read-only mode (allows only 'active' status)
def check_for_read_only_mode(
    current_user: TokenData = Depends(check_subscription_status)
):
    if current_user.role == UserRole.SYSTEM_OWNER:
        return current_user
        
    if current_user.subscription_status == SubscriptionStatus.GRACE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Subscription is in grace period. Write operations are disabled. Access is read-only."
        )
    return current_user

async def get_current_system_owner(current_user: TokenData = Depends(get_current_active_user)):
    if current_user.role != UserRole.SYSTEM_OWNER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough privileges: Requires System Owner role."
        )
    return current_user

async def get_current_corporate_admin_context(current_user: TokenData = Depends(check_subscription_status)):
    """
    Dependency that ensures the current user is a Corporate Admin and has an associated customer_id.
    """
    if current_user.role != UserRole.CORPORATE_ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough privileges: Requires Corporate Admin role."
        )
    if current_user.customer_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Corporate Admin user is not associated with a customer. Data integrity error."
        )
    return current_user

class HasPermission:
    """
    Dependency class to check if the current user has a specific permission.
    Permissions are loaded from the token (which are pulled from DB on login).
    """
    def __init__(self, permission_name: str):
        self.permission_name = permission_name

    async def __call__(self, current_user: TokenData = Depends(get_current_active_user)):
        if self.permission_name not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not enough permissions: Missing '{self.permission_name}'."
            )
        return current_user