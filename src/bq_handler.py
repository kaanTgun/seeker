# filepath: /Users/kaan/Development/seeker/src/bq_handler.py
import json
from google.cloud import bigquery
from google.api_core import exceptions
from src.logger import setup_logger
from src.uuid_handler import generate_show_id, generate_episode_id

log = setup_logger(__name__)

def check_episode_exists(client, project_id, dataset_id, podcast_name, episode_name):
    """
    Check if an episode already exists in BigQuery based on podcast name and episode title.
    
    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        podcast_name: Name of the podcast (used to generate show ID)
        episode_name: Title of the episode
        
    Returns:
        bool: True if episode exists, False otherwise
    """
    try:
        # Generate the same IDs that would be used for this episode
        show_id = generate_show_id(podcast_name)
        episode_id = generate_episode_id(show_id, episode_name)
        
        # Query to check if episode exists
        query = f"""
        SELECT COUNT(*) as count
        FROM `{project_id}.{dataset_id}.EPISODES`
        WHERE id = @episode_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("episode_id", "STRING", episode_id)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            return row.count > 0
            
        return False
        
    except Exception as e:
        log.error(f"Error checking if episode exists: {e}")
        return False


def check_show_exists(client, project_id, dataset_id, podcast_name):
    """
    Check if a show already exists in BigQuery based on podcast name.
    
    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        podcast_name: Name of the podcast (used to generate show ID)
        
    Returns:
        bool: True if show exists, False otherwise
    """
    try:
        # Generate the same ID that would be used for this show
        show_id = generate_show_id(podcast_name)
        
        # Query to check if show exists
        query = f"""
        SELECT COUNT(*) as count
        FROM `{project_id}.{dataset_id}.SHOWS`
        WHERE id = @show_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("show_id", "STRING", show_id)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            return row.count > 0
            
        return False
        
    except Exception as e:
        log.error(f"Error checking if show exists: {e}")
        return False


def insert_episode_data(client, project_id, dataset_id, episode_bq_data):
    """
    Insert episode data into BigQuery tables. Handles inserting into multiple tables:
    AUDIO, SHOWS (if not exists), EPISODES, PEOPLE (if not exists), SHOW_HOSTS, EPISODE_GUESTS.
    
    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        episode_bq_data: Dictionary containing all episode data structured for BigQuery
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        dataset_ref = client.dataset(dataset_id)
        
        # Insert core episode data (audio, show, episode)
        if not _insert_core_episode_data(client, project_id, dataset_id, dataset_ref, episode_bq_data):
            return False
        
        # Insert people and relationship data
        _insert_people_and_relationships(client, project_id, dataset_id, dataset_ref, episode_bq_data)
        
        log.info("Successfully inserted all episode data")
        return True
        
    except Exception as e:
        log.error(f"Error inserting episode data: {e}")
        return False


def _insert_core_episode_data(client, project_id, dataset_id, dataset_ref, episode_bq_data):
    """Insert audio, show, and episode records."""
    try:
        # 1. Insert AUDIO record
        audio_table = dataset_ref.table("AUDIO")
        log.info(f"Inserting audio record with ID: {episode_bq_data['audio']['id']}")
        errors = client.insert_rows_json(audio_table, [episode_bq_data['audio']])
        if errors:
            log.error(f"Error inserting audio data: {errors}")
            return False
        
        # 2. Insert SHOW record (if not exists)
        shows_table = dataset_ref.table("SHOWS")
        show_id = episode_bq_data['show']['id']
        if not _check_record_exists_by_id(client, project_id, dataset_id, "SHOWS", show_id):
            log.info(f"Inserting show record with ID: {show_id}")
            errors = client.insert_rows_json(shows_table, [episode_bq_data['show']])
            if errors:
                log.error(f"Error inserting show data: {errors}")
                return False
        else:
            log.info(f"Show with ID {show_id} already exists, skipping insert")
        
        # 3. Insert EPISODE record
        episodes_table = dataset_ref.table("EPISODES")
        log.info(f"Inserting episode record with ID: {episode_bq_data['episode']['id']}")
        errors = client.insert_rows_json(episodes_table, [episode_bq_data['episode']])
        if errors:
            log.error(f"Error inserting episode data: {errors}")
            return False
        
        return True
        
    except Exception as e:
        log.error(f"Error inserting core episode data: {e}")
        return False


def _insert_people_and_relationships(client, project_id, dataset_id, dataset_ref, episode_bq_data):
    """Insert people records and relationship data."""
    people_table = dataset_ref.table("PEOPLE")
    show_hosts_table = dataset_ref.table("SHOW_HOSTS")
    episode_guests_table = dataset_ref.table("EPISODE_GUESTS")
    
    # Insert PEOPLE records (if not exists)
    for person_data in episode_bq_data.get('people', []):
        _insert_person_if_not_exists(client, project_id, dataset_id, people_table, person_data)
    
    # Insert relationship records
    _insert_show_hosts(client, project_id, dataset_id, show_hosts_table, episode_bq_data.get('show_hosts', []))
    _insert_episode_guests(client, project_id, dataset_id, episode_guests_table, episode_bq_data.get('episode_guests', []))


def _insert_person_if_not_exists(client, project_id, dataset_id, people_table, person_data):
    """Insert a person record if it doesn't already exist."""
    person_id = person_data['id']
    if not _check_record_exists_by_id(client, project_id, dataset_id, "PEOPLE", person_id):
        log.info(f"Inserting person record with ID: {person_id}")
        errors = client.insert_rows_json(people_table, [person_data])
        if errors:
            log.error(f"Error inserting person data: {errors}")
    else:
        log.info(f"Person with ID {person_id} already exists, skipping insert")


def _insert_show_hosts(client, project_id, dataset_id, show_hosts_table, show_hosts_data):
    """Insert show host relationship records."""
    for show_host_data in show_hosts_data:
        if not _check_relationship_exists(client, project_id, dataset_id, "SHOW_HOSTS", 
                                        "showId", show_host_data['showId'], 
                                        "personId", show_host_data['personId']):
            log.info(f"Inserting show host relationship: show {show_host_data['showId']}, person {show_host_data['personId']}")
            errors = client.insert_rows_json(show_hosts_table, [show_host_data])
            if errors:
                log.error(f"Error inserting show host data: {errors}")


def _insert_episode_guests(client, project_id, dataset_id, episode_guests_table, episode_guests_data):
    """Insert episode guest relationship records."""
    for episode_guest_data in episode_guests_data:
        if not _check_relationship_exists(client, project_id, dataset_id, "EPISODE_GUESTS", 
                                        "episodeId", episode_guest_data['episodeId'], 
                                        "personId", episode_guest_data['personId']):
            log.info(f"Inserting episode guest relationship: episode {episode_guest_data['episodeId']}, person {episode_guest_data['personId']}")
            errors = client.insert_rows_json(episode_guests_table, [episode_guest_data])
            if errors:
                log.error(f"Error inserting episode guest data: {errors}")


def _check_record_exists_by_id(client, project_id, dataset_id, table_name, record_id):
    """
    Helper function to check if a record exists by ID.
    
    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Name of the table to check
        record_id: ID of the record to check
        
    Returns:
        bool: True if record exists, False otherwise
    """
    try:
        query = f"""
        SELECT COUNT(*) as count
        FROM `{project_id}.{dataset_id}.{table_name}`
        WHERE id = @record_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("record_id", "STRING", record_id)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            return row.count > 0
            
        return False
        
    except Exception as e:
        log.error(f"Error checking if record exists in {table_name}: {e}")
        return False


def _check_relationship_exists(client, project_id, dataset_id, table_name, col1_name, col1_value, col2_name, col2_value):
    """
    Helper function to check if a relationship record exists.
    
    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Name of the table to check
        col1_name: Name of the first column
        col1_value: Value of the first column
        col2_name: Name of the second column
        col2_value: Value of the second column
        
    Returns:
        bool: True if relationship exists, False otherwise
    """
    try:
        query = f"""
        SELECT COUNT(*) as count
        FROM `{project_id}.{dataset_id}.{table_name}`
        WHERE {col1_name} = @col1_value AND {col2_name} = @col2_value
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("col1_value", "STRING", col1_value),
                bigquery.ScalarQueryParameter("col2_value", "STRING", col2_value)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            return row.count > 0
            
        return False
        
    except Exception as e:
        log.error(f"Error checking if relationship exists in {table_name}: {e}")
        return False