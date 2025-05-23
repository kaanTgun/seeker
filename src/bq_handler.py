import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import src.utils as utils # Import utility functions
from src.logger import setup_logger # Import the custom logger

log = setup_logger(__name__) # Setup logger for this module

# Configuration for table IDs (consider moving to a config file or env vars)
EPISODES_TABLE_ID = "episodes"

def check_episode_exists(client, project_id, dataset_id, episode_original_audio_url):
    """
    Checks if an episode with the given original audio URL already exists in BigQuery.

    Args:
        client: The BigQuery client instance.
        project_id: The Google Cloud project ID.
        dataset_id: The BigQuery dataset ID.
        episode_original_audio_url: The original audio URL of the episode.

    Returns:
        True if the episode exists, False otherwise.
    """


    table_id = f"{project_id}.{dataset_id}.EPISODES"

    # Use a parameterized query to prevent SQL injection
    query = f"""
        SELECT
            COUNT(*)
        FROM
            `{table_id}`
        WHERE
            originalAudioUrl = @original_audio_url
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("original_audio_url", "STRING", episode_original_audio_url),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result() # Waits for job to complete.

        for row in results:
            if row[0] > 0:
                log.info(f"Episode with URL {episode_original_audio_url} already exists.")
                return True
        log.info(f"Episode with URL {episode_original_audio_url} not found.")
        return False

    except NotFound:
        log.warning(f"BigQuery table {table_id} not found. Assuming episode does not exist.")
        # If the table doesn't exist, no episodes exist yet.
        return False
    except Exception as e:
        log.error(f"An error occurred while checking episode existence in BigQuery: {e}")
        # In case of other errors, assume it doesn't exist to avoid blocking,
        # but log the error for investigation.
        return False

def insert_episode_data(client, project_id, dataset_id, data):
    """
    Inserts episode-related data into the BigQuery tables.

    Args:
        client: The BigQuery client instance.
        project_id: The Google Cloud project ID.
        dataset_id: The BigQuery dataset ID.
        data: A dictionary containing data for the different tables
              (audio, show, episode, people, topics, synthetic_topics,
               show_hosts, episode_guests).

    Returns:
        True if insertion is successful, False otherwise.
    """

    insert_success = True

    # Helper function to insert rows into a specific table
    def insert_rows(table_name, rows):
        if not rows:
            return True # Nothing to insert

        table_id = f"{project_id}.{dataset_id}.{table_name}"
        errors = client.insert_rows_json(table_id, rows)

        if errors:
            log.error(f"Errors inserting rows into {table_id}:")
            for error in errors:
                log.error(error)
            return False
        else:
            log.info(f"Successfully inserted {len(rows)} row(s) into {table_id}.")
            return True

    # Prepare data for insertion (ensure lists of dictionaries)
    audio_rows = [data["audio"]] if data.get("audio") else []
    show_rows = [data["show"]] if data.get("show") else []
    episode_rows = [data["episode"]] if data.get("episode") else []
    people_rows = data.get("people", [])
    topics_rows = data.get("topics", [])
    synthetic_topics_rows = data.get("synthetic_topics", [])
    show_hosts_rows = data.get("show_hosts", [])
    episode_guests_rows = data.get("episode_guests", [])

    # Perform insertions (order might matter for foreign key relationships,
    # but BigQuery doesn't enforce them, so logical order is fine)
    if not insert_rows("AUDIO", audio_rows): insert_success = False
    if not insert_rows("SHOWS", show_rows): insert_success = False
    if not insert_rows("EPISODES", episode_rows): insert_success = False
    if not insert_rows("PEOPLE", people_rows): insert_success = False
    if not insert_rows("TOPICS", topics_rows): insert_success = False
    if not insert_rows("SYNTHETIC_TOPICS", synthetic_topics_rows): insert_success = False
    if not insert_rows("SHOW_HOSTS", show_hosts_rows): insert_success = False
    if not insert_rows("EPISODE_GUESTS", episode_guests_rows): insert_success = False

    return insert_success
