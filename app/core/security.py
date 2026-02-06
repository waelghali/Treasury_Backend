# app/core/security.py
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import Depends, HTTPException, status, Request, Response
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session

from app.constants import UserRole, SubscriptionStatus
from app.database import get_db
from app.crud.crud import crud_user, crud_role_permission

# --- Configuration & Environment Variables ---

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 20))
TRUST_X_FORWARDED = os.getenv("TRUST_X_FORWARDED", "false").lower() == "true"

if SECRET_KEY is None:
    raise ValueError("SECRET_KEY environment variable is not set.")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v2/login", auto_error=False)


# --- Data Models ---

class TokenData(BaseModel):
    email: Optional[EmailStr] = None
    user_id: Optional[int] = None
    role: Optional[UserRole] = None
    permissions: List[str] = Field([], description="List of permission names associated with the user's role.")
    customer_id: Optional[int] = Field(None, description="Include customer_id in token data")
    has_all_entity_access: Optional[bool] = Field(True, description="True if user has access to all entities, False if restricted")
    entity_ids: List[int] = Field([], description="List of customer entity IDs this user has access to.")
    must_change_password: Optional[bool] = Field(False, description="True if user must change password on next login.")
    subscription_status: Optional[SubscriptionStatus] = Field(None, description="Current subscription status of the customer.")
    must_accept_policies: Optional[bool] = Field(False, description="True if user needs to accept legal policies.")
    last_accepted_legal_version: Optional[float] = Field(None, description="Version of legal artifacts last accepted.")
    is_mfa_verified: bool = Field(True, description="False if the user still needs to enter an email code.")

# --- Core Functions ---

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """Creates a JWT access token with an expiration time."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def get_client_ip(request: Request) -> str:
    # 1. Check for X-Forwarded-For (standard for Render/Vercel)
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        # Grab the first IP in the chain (the actual user)
        return x_forwarded_for.split(',')[0].strip()
    
    # 2. Check for X-Real-IP (used by some proxies)
    x_real_ip = request.headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip

    # 3. Fallback to the direct connection host
    if request.client and request.client.host:
        return request.client.host
        
    return "IP_NOT_FOUND"

# --- Dependencies ---


async def get_current_user(
    request: Request,
    response: Response,
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> TokenData:
    """
    Validates the JWT token, checks DB for user existence/active status, 
    and loads permissions. Supports token via Header or Query Param.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # 1. Extract Token (Header preference, fallback to Query Param)
    if token is None:
        token = request.query_params.get("token")
        if token is None:
            raise credentials_exception

    # 2. Decode and Parse Token
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        email: str = payload.get("sub")
        user_id: int = payload.get("user_id")
        role: str = payload.get("role")
        
        if email is None or user_id is None or role is None:
            raise credentials_exception

        # Load permissions from DB based on role (Ensures permissions are always up to date)
        db_permissions = crud_role_permission.get_permissions_for_role(db, role)
        permission_names = [p.name for p in db_permissions]
        
        # Handle Enum conversion safely
        sub_status = payload.get("subscription_status")
        if sub_status:
             sub_status = SubscriptionStatus(sub_status)

        token_data = TokenData(
            email=email,
            user_id=user_id,
            role=UserRole(role),
            permissions=permission_names,
            customer_id=payload.get("customer_id"),
            has_all_entity_access=payload.get("has_all_entity_access"),
            entity_ids=payload.get("entity_ids", []),
            must_change_password=payload.get("must_change_password"),
            subscription_status=sub_status,
            must_accept_policies=payload.get("must_accept_policies"),
            last_accepted_legal_version=payload.get("last_accepted_legal_version")
        )
    except (JWTError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload or subscription status.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 3. Verify User Exists and is Active
    user = crud_user.get(db, user_id)
    if user is None or user.is_deleted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is inactive or deleted.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return token_data


async def get_current_active_user(current_user: TokenData = Depends(get_current_user)) -> TokenData:
    """Ensures user is authenticated and enforces password change policy."""
    if current_user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    
    if current_user.role != UserRole.SYSTEM_OWNER and current_user.must_change_password:
         raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must change your password on first login."
        )
    return current_user


def check_subscription_status(current_user: TokenData = Depends(get_current_active_user)) -> TokenData:
    """Blocks access if subscription is EXPIRED (skips for System Owner)."""
    if current_user.role == UserRole.SYSTEM_OWNER:
        return current_user
        
    if current_user.subscription_status == SubscriptionStatus.EXPIRED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Subscription is expired. Access is denied. Please contact the system owner to renew."
        )
    return current_user


def check_for_read_only_mode(current_user: TokenData = Depends(check_subscription_status)) -> TokenData:
    """Blocks write access if subscription is in GRACE period."""
    if current_user.role == UserRole.SYSTEM_OWNER:
        return current_user
        
    if current_user.subscription_status == SubscriptionStatus.GRACE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Subscription is in grace period. Write operations are disabled. Access is read-only."
        )
    return current_user


async def get_current_system_owner(current_user: TokenData = Depends(get_current_active_user)) -> TokenData:
    """Restricts access to System Owners only."""
    if current_user.role != UserRole.SYSTEM_OWNER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough privileges: Requires System Owner role."
        )
    return current_user


async def get_current_corporate_admin_context(current_user: TokenData = Depends(check_subscription_status)) -> TokenData:
    """Restricts access to Corporate Admins and ensures data integrity."""
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


async def get_verified_user(current_user: TokenData = Depends(get_current_active_user)) -> TokenData:
    """Ensures user has completed MFA if required."""
    if not current_user.is_mfa_verified:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="MFA_REQUIRED: Please verify your device via the code sent to your email."
        )
    return current_user

class HasPermission:
    """Dependency to check for specific permissions."""
    def __init__(self, permission_name: str):
        self.permission_name = permission_name

    async def __call__(self, current_user: TokenData = Depends(get_current_active_user)) -> TokenData:
        if self.permission_name not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not enough permissions: Missing '{self.permission_name}'."
            )
        return current_user