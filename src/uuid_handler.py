"""
UUID generation functions for deterministic IDs across database tables.
This ensures that related entities always have the same UUIDs even if created separately.
"""

import uuid
import re
from src.utils import sanitize_title

# Define URL namespace for consistent UUID generation
URL_NAMESPACE = uuid.NAMESPACE_URL
BASE_URL = "https://audio-incite.com/"

# Use the existing sanitize_title from utils, but adjust for UUID-specific needs
def prepare_title_for_uuid(title):
    """
    Prepares a title specifically for UUID generation by ensuring consistency.
    This converts the output from utils.sanitize_title to a format suitable for
    generating consistent UUIDs (lowercase with hyphens as separators).
    
    Args:
        title: The title string to prepare
        
    Returns:
        Prepared title string for UUID generation
    """
    if not title:
        return ""
    
    # First use the standard sanitize_title from utils
    sanitized = sanitize_title(title)
    
    # Additional processing specific to UUID generation:
    # Convert to lowercase (important for URL-based UUID consistency)
    sanitized = sanitized.lower()
    
    # Replace underscores with hyphens for URL friendliness
    sanitized = sanitized.replace('_', '-')
    
    # Ensure no adjacent hyphens
    sanitized = re.sub(r'-+', '-', sanitized)
    
    # Remove leading/trailing hyphens
    sanitized = sanitized.strip('-')
    
    return sanitized

def generate_show_id(show_title):
    """
    Generate a deterministic UUID for a show based on its title.
    
    Args:
        show_title: The title of the show
        
    Returns:
        UUID hex string
    """
    sanitized = prepare_title_for_uuid(show_title)
    url = f"{BASE_URL}shows/{sanitized}"
    return uuid.uuid3(URL_NAMESPACE, url).hex

def generate_episode_id(show_id, episode_title):
    """
    Generate a deterministic UUID for an episode based on show_id and episode title.
    
    Args:
        show_id: The UUID of the show this episode belongs to
        episode_title: The title of the episode
        
    Returns:
        UUID hex string
    """
    sanitized = prepare_title_for_uuid(episode_title)
    url = f"{BASE_URL}shows/{show_id}/episodes/{sanitized}"
    return uuid.uuid3(URL_NAMESPACE, url).hex

def generate_person_id(person_name):
    """
    Generate a deterministic UUID for a person based on their name.
    
    Args:
        person_name: The person's full name
        
    Returns:
        UUID hex string
    """
    sanitized = prepare_title_for_uuid(person_name)
    url = f"{BASE_URL}people/{sanitized}"
    return uuid.uuid3(URL_NAMESPACE, url).hex

def generate_audio_id(related_entity_id, audio_type="episode"):
    """
    Generate a deterministic UUID for an audio file based on related entity and type.
    
    Args:
        related_entity_id: The UUID of the related entity (show, episode, person)
        audio_type: Type of audio (episode, show, person)
        
    Returns:
        UUID hex string
    """
    url = f"{BASE_URL}{audio_type}/{related_entity_id}/audio"
    return uuid.uuid3(URL_NAMESPACE, url).hex

def generate_topic_id(episode_id, topic_title, start_ms):
    """
    Generate a deterministic UUID for a topic based on episode ID, title and start time.
    
    Args:
        episode_id: The UUID of the episode this topic belongs to
        topic_title: The title of the topic
        start_ms: The start time in milliseconds
        
    Returns:
        UUID hex string
    """
    sanitized = prepare_title_for_uuid(topic_title)
    url = f"{BASE_URL}episodes/{episode_id}/topics/{sanitized}/{start_ms}"
    return uuid.uuid3(URL_NAMESPACE, url).hex
