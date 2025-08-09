# core/encryption.py
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables (ensure this is called in main.py or your app entry point)
# load_dotenv()

# --- IMPORTANT: KEY MANAGEMENT ---
# The encryption key MUST be a securely generated, base64-encoded 32-byte key.
# It MUST be stored as an environment variable (e.g., APP_ENCRYPTION_KEY).
#
# To generate a key (RUN THIS ONCE, copy the output, and set it in your .env):
# from cryptography.fernet import Fernet
# key = Fernet.generate_key()
# print(key.decode()) # This is your APP_ENCRYPTION_KEY
#
# NEVER hardcode this key in your source code.

_encryption_key = os.getenv("APP_ENCRYPTION_KEY")

if _encryption_key is None:
    # In a production environment, you should raise an error or halt startup.
    # For development, we'll print a warning.
    print("WARNING: APP_ENCRYPTION_KEY environment variable is not set. Encryption/decryption will fail.")
    _fernet = None # Ensure Fernet is not initialized without a key
else:
    try:
        _fernet = Fernet(_encryption_key)
    except Exception as e:
        print(f"ERROR: Invalid APP_ENCRYPTION_KEY. Please ensure it's a valid Fernet key. Error: {e}")
        _fernet = None

def encrypt_data(data: str) -> str:
    """Encrypts a string using the configured Fernet key."""
    if _fernet is None:
        raise ValueError("Encryption key not initialized. Cannot encrypt data.")
    return _fernet.encrypt(data.encode()).decode()

def decrypt_data(encrypted_data: str) -> str:
    """Decrypts an encrypted string using the configured Fernet key."""
    if _fernet is None:
        raise ValueError("Encryption key not initialized. Cannot decrypt data.")
    try:
        return _fernet.decrypt(encrypted_data.encode()).decode()
    except Exception as e:
        print(f"ERROR: Failed to decrypt data. Key mismatch or corrupted data? Error: {e}")
        raise ValueError("Decryption failed. Data might be corrupted or key is incorrect.") from e

# --- Key Generation Helper (RUN ONCE, then remove or comment out for production) ---
def generate_fernet_key():
    """Generates and prints a new Fernet key."""
    key = Fernet.generate_key()
    print("\n--- GENERATE NEW APP_ENCRYPTION_KEY ---")
    print("Copy this key and add it to your .env file as APP_ENCRYPTION_KEY:")
    print(f"APP_ENCRYPTION_KEY={key.decode()}")
    print("----------------------------------------\n")
    return key.decode()

#if __name__ == "__main__":
    # This block runs only when encryption.py is executed directly.
    # Use it to generate your key ONCE.
    # After generating and setting the key in .env, you can remove or comment out this call.
    generate_fernet_key()

    # Example usage (after setting APP_ENCRYPTION_KEY in .env and restarting Python)
    # os.environ["APP_ENCRYPTION_KEY"] = "YOUR_GENERATED_KEY_HERE" # For testing this block directly
    #
    # try:
    #     test_data = "mysecretpassword123"
    #     print(f"Original: {test_data}")
    #     encrypted = encrypt_data(test_data)
    #     print(f"Encrypted: {encrypted}")
    #     decrypted = decrypt_data(encrypted)
    #     print(f"Decrypted: {decrypted}")
    #     assert test_data == decrypted
    #     print("Encryption/Decryption test successful!")
    # except ValueError as e:
    #     print(f"Test failed: {e}")