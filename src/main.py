import os
import json
from dotenv import load_dotenv
from http.client import UNAUTHORIZED, OK, INTERNAL_SERVER_ERROR, BAD_REQUEST

from src.logger import setup_logger # Import the custom logger
log = setup_logger(__name__) # Setup logger for this module

# Attempt to load environment variables from .env file for local development
# This is useful for local testing but won't be used in Cloud Functions environment
load_dotenv()

import src.auth_handler as auth_handler
import src.rss_parser as rss_parser
import src.gcs_handler as gcs_handler
import src.bq_handler as bq_handler

from google.cloud import bigquery, storage
from google.oauth2 import service_account

# Configuration from environment variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
PODCASTS_CONFIG_PATH = "config/podcasts.json" # Path relative to the function's root

# Basic validation of configuration
if not all([GCP_PROJECT_ID, GCS_BUCKET_NAME, BIGQUERY_DATASET_ID]):
    log.warning("Missing required environment variables: GCP_PROJECT_ID, GCS_BUCKET_NAME, BIGQUERY_DATASET_ID")

# Initialize Google Cloud clients
credentials = None
credentials_json_str = os.environ.get('GOOGLE_CREDENTIALS_JSON')

if credentials_json_str:
    try:
        with open(credentials_json_str, 'r') as f:
            credentials_info_str = f.read()
            credentials_info = json.loads(credentials_info_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        log.info("Successfully loaded credentials from GOOGLE_CREDENTIALS_JSON.")
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")

try:
    BQCLIENT = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    SBCLIENT = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
    log.info("Google Cloud clients initialized.")
except Exception as e:
    log.error(f"Failed to initialize Google Cloud clients: {e}")
    # Depending on the desired behavior, you might want to raise an exception here
    # or ensure that parts of the function that rely on these clients are not executed.
    BQCLIENT = None
    SBCLIENT = None


def _authenticate_request(request):
    """Handles authentication for the request."""
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        log.warning("Authorization header missing.")
        return None, ({"message": "Authorization header missing."}, UNAUTHORIZED)

    parts = auth_header.split()
    if parts[0].lower() != 'bearer' or len(parts) != 2:
        log.warning("Invalid Authorization header format.")
        return None, ({"message": "Invalid Authorization header format."}, UNAUTHORIZED)

    id_token = parts[1]
    decoded_token = auth_handler.verify_firebase_token(id_token)
    if not decoded_token:
        log.warning("Firebase token verification failed.")
        return None, ({"message": "Invalid or expired token."}, UNAUTHORIZED)

    log.info(f"User authenticated: {decoded_token.get('uid')}")
    return decoded_token, None

def _parse_and_validate_payload(request):
    """Parses and validates the JSON payload from the request."""
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            log.warning("Request payload is missing or not valid JSON.")
            return None, None, None, ({"message": "Request payload is missing or not valid JSON."}, BAD_REQUEST)

        podcast_name = request_json.get("podcast_name")
        num_episodes = request_json.get("num_episodes")
        force = request_json.get("force", False)

        if not podcast_name:
            log.warning("Missing 'podcast_name' in request payload.")
            return None, None, None, ({"message": "Missing 'podcast_name' in request payload."}, BAD_REQUEST)
        if num_episodes is None:
            log.warning("Missing 'num_episodes' in request payload.")
            return None, None, None, ({"message": "Missing 'num_episodes' in request payload."}, BAD_REQUEST)
        if not isinstance(num_episodes, int) or num_episodes < 0:
            log.warning("Invalid 'num_episodes': must be a non-negative integer.")
            return None, None, None, ({"message": "Invalid 'num_episodes': must be a non-negative integer."}, BAD_REQUEST)
        if not isinstance(force, bool):
            log.warning("Invalid 'force': must be a boolean value.")
            force = False  # Default to False if invalid
        
        return podcast_name, num_episodes, force, None
    except Exception as e:
        log.error(f"Error parsing request JSON: {e}")
        return None, None, None, ({"message": f"Error parsing request JSON: {e}"}, BAD_REQUEST)

def _load_podcasts_config():
    """Loads the podcasts configuration file."""
    try:
        with open(PODCASTS_CONFIG_PATH, 'r') as f:
            return json.load(f), None
    except FileNotFoundError:
        log.error(f"Podcasts config file not found at {PODCASTS_CONFIG_PATH}")
        return None, ({"message": f"Configuration file not found: {PODCASTS_CONFIG_PATH}"}, INTERNAL_SERVER_ERROR)
    except json.JSONDecodeError:
        log.error(f"Error decoding JSON from {PODCASTS_CONFIG_PATH}")
        return None, ({"message": f"Error decoding configuration file: {PODCASTS_CONFIG_PATH}"}, INTERNAL_SERVER_ERROR)
    except Exception as e:
        log.error(f"An error occurred reading config file: {e}")
        return None, ({"message": f"An error occurred reading configuration: {e}"}, INTERNAL_SERVER_ERROR)

def _fetch_and_sort_podcast_entries(podcast_name, rss_url):
    """Fetches podcast entries from RSS feed and sorts them by published date."""
    log.info(f"Processing feed for '{podcast_name}' from {rss_url}")
    feed = rss_parser.fetch_and_parse_feed(rss_url)

    if not feed or not feed.entries:
        log.warning(f"Could not fetch or parse feed for '{podcast_name}' or no entries found.")
        return None, None

    # Sort entries by published date, most recent first
    sorted_entries = sorted(
        feed.entries,
        key=lambda entry: entry.get('published_parsed') or (0,), # Use 0 if date parsing fails
        reverse=True
    )
    return feed, sorted_entries

def _get_episode_audio_url(entry):
    """Extracts the audio URL from a podcast entry."""
    if 'enclosures' in entry:
        for enclosure in entry['enclosures']:
            if enclosure.get('type', '').startswith('audio/'):
                return enclosure.get('url')
    return None

def _process_single_episode(entry, podcast_data, podcast_name, feed, force=False):
    """
    Processes a single podcast episode: download, GCS upload, BigQuery insert.
    
    Args:
        entry: The feed entry containing episode data
        podcast_data: Configuration data for the podcast
        podcast_name: Name of the podcast
        feed: The complete feed object
        force: If True, process the episode even if it already exists in BigQuery
        
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    episode_original_audio_url = _get_episode_audio_url(entry)
    episode_name = entry.get('title', 'Unknown')

    if not episode_original_audio_url:
        log.info(f"Skipping entry '{episode_name}' due to missing audio URL.")
        return False # Indicates skipping, not necessarily an error for the whole feed

    # Check if episode already exists in BigQuery
    if not force and bq_handler.check_episode_exists(BQCLIENT, GCP_PROJECT_ID, BIGQUERY_DATASET_ID, podcast_name, episode_name):
        log.info(f"Episode '{episode_name}' from podcast '{podcast_name}' already processed. Skipping.")
        return False # Indicates skipping

    episode_bq_data = rss_parser.extract_episode_data(entry, podcast_data, podcast_name, feed)
    if not episode_bq_data:
        log.warning(f"Failed to extract data for episode '{episode_name}'. Skipping.")
        return False # Indicates skipping

    audio_data = gcs_handler.download_file(episode_original_audio_url)
    if not audio_data:
        log.warning(f"Failed to download audio for episode '{episode_bq_data['episode']['title']}'. Skipping.")
        return False # Indicates skipping

    episode_bq_data['audio']['fileSize'] = len(audio_data)
    gcs_object_path = gcs_handler.construct_gcs_object_path(
        episode_bq_data['show']['title'],
        episode_bq_data['episode']['title']
    )
    episode_bq_data['audio']['gcsBucket'] = GCS_BUCKET_NAME
    episode_bq_data['audio']['gcsObjectPath'] = gcs_object_path

    if not gcs_handler.upload_to_gcs(SBCLIENT, GCS_BUCKET_NAME, gcs_object_path, audio_data):
        log.error(f"Failed to upload audio for episode '{episode_bq_data['episode']['title']}' to GCS. Skipping BigQuery insert.")
        return False # Critical failure for this episode

    if bq_handler.insert_episode_data(BQCLIENT, GCP_PROJECT_ID, BIGQUERY_DATASET_ID, episode_bq_data):
        log.info(f"Successfully processed and inserted data for episode '{episode_bq_data['episode']['title']}'.")
        return True # Success for this episode
    else:
        log.error(f"Failed to insert data for episode '{episode_bq_data['episode']['title']}' into BigQuery.")
        return False # Critical failure for this episode

def process_podcast_feed(podcast_name, podcast_data, limit=2, force=False):
    """
    Fetches, parses, and processes a single podcast feed.
    
    Args:
        podcast_name: Name of the podcast to process
        podcast_data: Configuration data for the podcast
        limit: Maximum number of episodes to process
        force: If True, process episodes even if they already exist in BigQuery
        
    Returns:
        bool: True if overall processing succeeded, False otherwise
    """
    rss_url = podcast_data.get("rss")
    if not rss_url:
        log.warning(f"Skipping podcast '{podcast_name}': Missing RSS URL in config.")
        return False

    # Check if show exists in BigQuery
    if not bq_handler.check_show_exists(BQCLIENT, GCP_PROJECT_ID, BIGQUERY_DATASET_ID, podcast_name):
        log.info(f"Show '{podcast_name}' not found in BigQuery. Will be created during episode processing.")
        # Note: We don't need to create it here as insert_episode_data will handle it
        # due to the ordered table insertion (SHOWS first, then EPISODES)

    feed, sorted_entries = _fetch_and_sort_podcast_entries(podcast_name, rss_url)
    if not sorted_entries:
        return False # Error already logged by helper

    processed_count = 0
    for entry in sorted_entries:
        if _process_single_episode(entry, podcast_data, podcast_name, feed, force=force):
            processed_count += 1
        
        # Even if an episode fails processing (_process_single_episode returns False),
        # we continue to the next, but it doesn't count towards the limit.
        # The limit is for *successfully* processed new episodes.
        if processed_count >= limit:
            log.info(f"Reached processing limit of {limit} episodes for '{podcast_name}'.")
            break
            
    log.info(f"Finished processing feed for '{podcast_name}'. Successfully processed {processed_count} new episodes.")
    return True # Overall success, even if some individual episodes failed but were skipped

def cloud_function_entrypoint(request):
    """
    Main entry point for the Cloud Function.
    Handles HTTP requests, authentication, and triggers podcast processing.
    """
    # Check if running in a deployed Cloud Function environment
    # K_SERVICE is a common environment variable in Cloud Run/Cloud Functions (2nd gen)
    is_deployed = os.getenv("K_SERVICE") is not None

    if is_deployed:
        log.info("Running in deployed environment, authentication required.")
        _, error_response = _authenticate_request(request)
        if error_response:
            return error_response
    else:
        log.info("Running in local development environment, skipping authentication.")

    podcast_name_to_process, num_episodes, force, error_response = _parse_and_validate_payload(request)
    if error_response:
        return error_response

    podcasts_config, error_response = _load_podcasts_config()
    if error_response:
        return error_response

    if podcast_name_to_process not in podcasts_config:
        log.warning(f"Podcast '{podcast_name_to_process}' not found in configuration.")
        return {"message": f"Podcast '{podcast_name_to_process}' not found in configuration."}, BAD_REQUEST

    podcast_data = podcasts_config[podcast_name_to_process]
    
    if process_podcast_feed(podcast_name_to_process, podcast_data, limit=num_episodes, force=force):
        return {"message": f"Successfully processed {num_episodes} episodes for '{podcast_name_to_process}'."}, OK
    else:
        return {"message": f"Failed to process podcast feed for '{podcast_name_to_process}'."}, INTERNAL_SERVER_ERROR


# Example of how to run locally (for testing purposes)
# This part is typically not included when deploying to Cloud Functions
if __name__ == "__main__":
    # Mock request object for local testing
    class MockRequest:
        def __init__(self, headers=None, get_json_data=None): # Renamed get_json to get_json_data
            self.headers = headers or {}
            self._get_json_data = get_json_data # Renamed

        def get_json(self, silent=True): # Removed unused 'force', 'silent' is used by the caller
            # The 'silent' parameter in the actual request.get_json() method
            # influences error handling (e.g., raising an exception vs. returning None).
            # Here, we simplify and assume if _get_json_data is present, it's a success.
            # If it's None, it simulates a case where get_json(silent=True) would return None.
            if self._get_json_data:
                return self._get_json_data()
            return None


    log.info("Running locally...")

    TEST_PODCAST_NAME = "Diary of a CEO" # Define constant for "All In"

    # Example of an authenticated request
    log.info("--- Testing Authenticated Request ---")
    mock_headers_authenticated = {"Authorization": "Bearer YOUR_FIREBASE_ID_TOKEN"}
    mock_request_authenticated_payload = {"podcast_name": TEST_PODCAST_NAME, "num_episodes": 2}
    mock_request_authenticated = MockRequest(headers=mock_headers_authenticated, get_json_data=lambda: mock_request_authenticated_payload)
    response, status = cloud_function_entrypoint(mock_request_authenticated)
    log.info(f"Response: {response}, Status: {status}")