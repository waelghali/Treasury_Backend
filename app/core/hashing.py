# c:\Grow\core\hashing.py
from passlib.context import CryptContext

# --- Password Hashing Context ---
# This module is solely responsible for password hashing/verification.
# It should not import anything that could lead to circular dependencies.
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

# --- Password Hashing and Verification Functions ---
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifies a plain password against a hashed password using the configured passlib context.
    This is the primary function used by the authentication endpoint.
    """
    # print(f"DEBUG: verify_password (auth) - Plain: [HIDDEN], Hashed: {hashed_password}") # Keep debug if needed
    try:
        result = pwd_context.verify(plain_password, hashed_password)
        # print(f"DEBUG: verify_password (auth) - Result: {result}") # Keep debug if needed
        return result
    except ValueError as e:
        # print(f"DEBUG: ValueError during password verification (auth): {e}") # Keep debug if needed
        return False
    except Exception as e:
        # print(f"DEBUG: Unexpected error during password verification (auth): {e}") # Keep debug if needed
        return False

def verify_password_direct(plain_password: str, hashed_password: str) -> bool:
    """
    Verifies a plain password against a hashed password, explicitly specifying the scheme.
    This is used by the User model's check_password method to ensure consistency.
    """
    # print(f"DEBUG: verify_password_direct (model) - Plain: [HIDDEN], Hashed: {hashed_password}") # Keep debug if needed
    try:
        result = pwd_context.verify(plain_password, hashed_password, schemes=["pbkdf2_sha256"])
        # print(f"DEBUG: verify_password_direct (model) - Result: {result}") # Keep debug if needed
        return result
    except ValueError as e:
        # print(f"DEBUG: ValueError during password verification (model): {e}") # Keep debug if needed
        return False
    except Exception as e:
        # print(f"DEBUG: Unexpected error during password verification (model): {e}") # Keep debug if needed
        return False

def get_password_hash(password: str) -> str:
    """Hashes a plain password."""
    hashed = pwd_context.hash(password)
    # print(f"DEBUG: get_password_hash - Hashed: {hashed}") # Keep debug if needed
    return hashed

