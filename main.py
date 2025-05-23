# main.py - Entry point for Cloud Functions deployment
# This file exists to satisfy Cloud Functions' requirement for main.py in the root directory

# Import the actual implementation from src/
from src.main import cloud_function_entrypoint

# Re-export the function for Cloud Functions to find
__all__ = ['cloud_function_entrypoint']
