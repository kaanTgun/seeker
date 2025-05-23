import uuid
import re
from logger import setup_logger # Import the custom logger

log = setup_logger(__name__) # Setup logger for this module

def generate_uuid():
    """Generates a unique UUID."""
    return str(uuid.uuid4())

def sanitize_title(title):
    """Sanitizes a string for use in file paths or identifiers."""
    # Replace spaces with underscores
    sanitized = title.replace(" ", "_")
    # Remove characters that are not alphanumeric, underscores, or hyphens
    sanitized = re.sub(r'[^\w\s.-]', '', sanitized)
    # Replace multiple underscores/hyphens with a single one
    sanitized = re.sub(r'[_.-]+', '_', sanitized)
    # Remove leading/trailing underscores/hyphens
    sanitized = sanitized.strip('_-')
    return sanitized

# Add other utility functions as needed
