import os # Add os import
import firebase_admin
from firebase_admin import credentials, auth
from logger import setup_logger # Import the custom logger

log = setup_logger(__name__) # Setup logger for this module

def initialize_auth(credentials_path=None):
    """
    Initialize Firebase Admin SDK with optional credentials file path.
    
    Args:
        credentials_path: Optional path to credentials.json file.
    """
    if not firebase_admin._apps:
        try:
            if credentials_path and os.path.exists(credentials_path):
                # Use provided credentials file
                cred = credentials.Certificate(credentials_path)
            else:
                # Fall back to application default credentials
                cred = credentials.ApplicationDefault()
            
            firebase_admin.initialize_app(cred)
            log.info("Firebase Admin SDK initialized.") # Replaced print with log.info
        except Exception as e:
            log.error(f"Error initializing Firebase Admin SDK: {e}") # Replaced print with log.error
            # Depending on the use case, you might want to raise the error
            # or handle it in a way that allows the application to continue
            # if Firebase auth is not strictly required for all operations.

# Initialize with default credentials when module is imported
initialize_auth()

def verify_firebase_token(id_token):
    """
    Verifies the Firebase ID token.

    Args:
        id_token: The ID token string.

    Returns:
        The decoded token dictionary if valid, None otherwise.
    """
    if not firebase_admin._apps:
        log.warning("Firebase Admin SDK not initialized. Cannot verify token.")
        return None
    try:
        # Verify the ID token while checking if the token is revoked.
        decoded_token = auth.verify_id_token(id_token)
        return decoded_token
    except firebase_admin.auth.ExpiredIdTokenError:
        log.warning("Firebase ID token has expired.") # Replaced print with log.warning
        return None
    except firebase_admin.auth.InvalidIdTokenError:
        log.warning("Firebase ID token is invalid.") # Replaced print with log.warning
        return None
    except Exception as e:
        log.error(f"An unexpected error occurred during token verification: {e}") # Replaced print with log.error
        return None
