import os
import requests
from google.cloud import storage
import src.utils as utils # Import utility functions
from src.logger import setup_logger # Import the custom logger
log = setup_logger(__name__) # Setup logger for this module

def download_file(url):
    """
    Downloads a file from a given URL.

    Args:
        url: The URL of the file to download.

    Returns:
        The content of the file as bytes if successful, None otherwise.
    """
    try:
        response = requests.get(url, stream=True, timeout=30) # Use stream=True for potentially large files
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.content
    except requests.exceptions.RequestException as e:
        log.error(f"Error downloading file from {url}: {e}") # Replaced print with log.error
        return None
    except Exception as e:
        log.error(f"An unexpected error occurred while downloading {url}: {e}") # Replaced print with log.error
        return None

def upload_to_gcs(client, bucket_name, destination_blob_name, data):
    """
    Uploads data to a Google Cloud Storage bucket.

    Args:
        client: The Google Cloud Storage client instance.
        bucket_name: The name of the GCS bucket.
        destination_blob_name: The path/name of the object in the bucket.
        data: The content to upload (bytes).

    Returns:
        True if upload is successful, False otherwise.
    """
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(data)

        log.info(f"File uploaded to gs://{bucket_name}/{destination_blob_name}") # Replaced print with log.info
        return True
    except Exception as e:
        log.error(f"Error uploading to GCS bucket {bucket_name}, blob {destination_blob_name}: {e}") # Replaced print with log.error
        return False

def construct_gcs_object_path(show_title, episode_title):
    """
    Constructs the GCS object path based on show and episode titles.

    Args:
        show_title: The title of the show.
        episode_title: The title of the episode.

    Returns:
        The constructed GCS object path string.
    """
    sanitized_show_title = utils.sanitize_title(show_title)
    sanitized_episode_title = utils.sanitize_title(episode_title)
    # Format: audio/<Sanitized Show Title>/<Sanitized Episode Name>.mp3
    return f"audio/{sanitized_show_title}/{sanitized_episode_title}.mp3"
