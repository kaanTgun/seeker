import feedparser
import requests
from datetime import datetime, timezone # Import timezone
from dateutil import parser as date_parser # Using dateutil for robust date parsing
from src.logger import setup_logger # Import the custom logger
import src.utils as utils
from src.uuid_handler import generate_show_id, generate_episode_id, generate_audio_id, generate_person_id

log = setup_logger(__name__) # Setup logger for this module

def fetch_and_parse_feed(rss_url):
    """
    Fetches and parses an RSS feed.

    Args:
        rss_url: The URL of the RSS feed.

    Returns:
        A feedparser object if successful, None otherwise.
    """
    try:
        # Use requests to fetch the feed content
        response = requests.get(rss_url, timeout=10) # Add a timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        feed_content = response.content

        # Parse the feed content
        feed = feedparser.parse(feed_content)

        # Check for parsing errors
        if feed.bozo:
            log.warning(f"Warning: Feed at {rss_url} may be ill-formed. Bozo reason: {feed.bozo_exception}") # Replaced print with log.warning
            # Depending on the error, you might want to return None or the partial feed

        return feed

    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching RSS feed {rss_url}: {e}") # Replaced print with log.error
        return None
    except Exception as e:
        log.error(f"An unexpected error occurred while fetching/parsing {rss_url}: {e}") # Replaced print with log.error
        return None

def parse_duration_to_seconds(duration_str):
    """
    Parses a duration string (MM:SS or HH:MM:SS) into total seconds.
    Returns None if parsing fails or input is invalid.
    """
    if not duration_str:
        return None
    try:
        parts = list(map(int, duration_str.split(':')))
        if len(parts) == 2: # MM:SS
            return parts[0] * 60 + parts[1]
        elif len(parts) == 3: # HH:MM:SS
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        else:
            log.warning(f"Warning: Unrecognized duration format: {duration_str}")
            return None
    except ValueError:
        log.warning(f"Warning: Could not parse duration string as integers: {duration_str}")
        return None
    except Exception as e:
        log.error(f"An unexpected error occurred parsing duration '{duration_str}': {e}")
        return None

# Add configured_podcast_name to the function signature
# pylint: disable=C901
def extract_episode_data(feed_entry, show_data, configured_podcast_name):
    """
    Extracts relevant data from a single feed entry and maps it to BigQuery schema.

    Args:
        feed_entry: A single entry from the parsed feed.
        show_data: A dictionary containing data about the show (from podcasts.json).
        configured_podcast_name: The name of the podcast as per the configuration key.

    Returns:
        A dictionary containing mapped data for BigQuery tables, or None if essential data is missing.
    """
    try:
        # --- Extract essential data ---
        episode_title = feed_entry.get('title')
        published_date_str = feed_entry.get('published')
        audio_url = None
        # Find the audio enclosure link
        if 'enclosures' in feed_entry:
            for enclosure in feed_entry['enclosures']:
                if enclosure.get('type', '').startswith('audio/'):
                    audio_url = enclosure.get('url')
                    break # Take the first audio enclosure found

        if not all([episode_title, published_date_str, audio_url]):
            log.warning(f"Skipping episode due to missing essential data: Title='{episode_title}', Published='{published_date_str}', Audio URL='{audio_url}'")
            return None

        # --- Parse Published Date ---
        try:
            # Use dateutil for robust parsing of various date formats
            published_date = date_parser.parse(published_date_str)
            # Ensure timezone-aware datetime for BigQuery TIMESTAMP
            if published_date.tzinfo is None:
                 # Assume UTC if no timezone info is present, or handle as appropriate
                 # For simplicity, let's assume UTC if not specified
                 log.warning(f"Warning: No timezone info for episode '{episode_title}'. Assuming UTC.")
                 published_date = published_date.replace(tzinfo=timezone.utc) # Corrected
            # Convert to UTC if it's not already
            published_date_utc = published_date.astimezone(timezone.utc) # Corrected

        except Exception as e:
            log.error(f"Error parsing published date '{published_date_str}' for episode '{episode_title}': {e}")
            return None # Skip episode if date parsing fails

        # --- Generate UUIDs using uuid_handler ---
        show_id = generate_show_id(feed_show_title)
        episode_id = generate_episode_id(show_id, episode_title)
        audio_id = generate_audio_id(episode_id)

        # --- Map data to BigQuery Schema ---

        # AUDIO Table Data
        audio_data = {
            "id": audio_id,
            "gcsBucket": None, # Will be filled later
            "gcsObjectPath": None, # Will be filled later
            "fileSize": None # Will be filled later (requires downloading the file)
        }

        # SHOWS Table Data (Extract from feed or show_data)
        # Determine the show title with improved fallback logic:
        # 1. From show_data (podcasts.json specific "title" field for the podcast)
        # 2. From the configured_podcast_name (the key from podcasts.json, e.g., "All In")
        # 3. From the feed entry's embedded feed info (less reliable for overall feed title)
        # 4. Default to 'Unknown Show'
        title_from_config_value = show_data.get('title')
        title_from_feed_entry_feed = feed_entry.get('feed', {}).get('title')

        if title_from_config_value:
            feed_show_title = title_from_config_value
        elif configured_podcast_name: # Use the key from podcasts.json if "title" field is missing
            feed_show_title = configured_podcast_name
        elif title_from_feed_entry_feed: # Less reliable, but a fallback
            feed_show_title = title_from_feed_entry_feed
        else:
            feed_show_title = 'Unknown Show' # Absolute fallback


        # Original logic for other show details, using the determined feed_show_title
        feed_show_description = feed_entry.get('feed', {}).get('subtitle', feed_entry.get('feed', {}).get('summary', ''))
        feed_show_image_url = feed_entry.get('feed', {}).get('image', {}).get('href', '')
        feed_show_website_url = feed_entry.get('feed', {}).get('link', '')
        feed_show_language = feed_entry.get('feed', {}).get('language', '')
        feed_show_tags = [tag['term'] for tag in feed_entry.get('feed', {}).get('tags', [])] + \
                         [cat['term'] for cat in feed_entry.get('feed', {}).get('categories', [])]
        # Remove duplicates
        feed_show_tags = list(set(feed_show_tags))


        show_bq_data = {
            "id": show_id,
            "title": feed_show_title,
            "sanitizedTitle": utils.sanitize_title(feed_show_title),
            "description": feed_show_description,
            "imageUrl": feed_show_image_url,
            "rssUrl": show_data.get('rss'), # Use the URL from the config
            "websiteUrl": feed_show_website_url,
            "language": feed_show_language,
            "tags": feed_show_tags,
            "lastUpdated": datetime.now(timezone.utc).isoformat() # Use current time for last updated
        }

        # EPISODES Table Data
        episode_description = feed_entry.get('summary', feed_entry.get('description', ''))
        episode_duration_str = feed_entry.get('itunes_duration') # Common field for duration
        episode_duration_seconds = parse_duration_to_seconds(episode_duration_str) # Use the new parsing function

        episode_bq_data = {
            "id": episode_id,
            "showId": show_id, # Link to the show
            "title": episode_title,
            "sanitizedTitle": utils.sanitize_title(episode_title),
            "description": episode_description,
            "publishedDate": published_date_utc.isoformat(), # Use ISO format for BigQuery TIMESTAMP
            "durationSeconds": episode_duration_seconds, # Use the parsed seconds
            "originalAudioUrl": audio_url,
            "audioId": audio_id # Link to the audio
        }

        # PEOPLE Table Data (Hosts/Guests)
        # This is more complex as RSS feeds vary greatly in how they list authors/contributors.
        # feedparser tries to normalize this in entry.authors and entry.contributors
        people_data = []
        show_hosts_data = []
        episode_guests_data = []

        # Process authors (often hosts)
        if 'authors' in feed_entry:
            for author in feed_entry['authors']:
                full_name = author.get('name', 'Unknown Author')
                person_id = generate_person_id(full_name)
                people_data.append({
                    "id": person_id,
                    "full_name": full_name,
                    "aliases": author.get('email'), # Sometimes email is used as an alias
                    "audioId": None # This field seems less relevant for hosts/guests in this schema?
                                    # Based on schema, audioId is nullable, maybe for speakers *within* an audio file?
                                    # Leaving as None for now based on schema interpretation for hosts/guests.
                })
                # Assuming authors are show hosts for simplicity, adjust if needed
                show_hosts_data.append({
                    "showId": show_id,
                    "personId": person_id
                })

        # Process contributors (often guests)
        if 'contributors' in feed_entry:
             for contributor in feed_entry['contributors']:
                full_name = contributor.get('name', 'Unknown Contributor')
                person_id = generate_person_id(full_name)
                people_data.append({
                    "id": person_id,
                    "full_name": full_name,
                    "aliases": contributor.get('email'),
                    "audioId": None # See note above
                })
                episode_guests_data.append({
                    "episodeId": episode_id,
                    "personId": person_id
                })

        # TOPICS and SYNTHETIC_TOPICS - These are not typically in standard RSS feeds.
        # The schema suggests these might be generated later (e.g., from transcription/analysis).
        # We will return empty lists for now, as the RSS feed won't contain this data.
        topics_data = []
        synthetic_topics_data = []


        return {
            "audio": audio_data,
            "show": show_bq_data,
            "episode": episode_bq_data,
            "people": people_data, # List of people found (hosts/guests)
            "topics": topics_data, # Empty list
            "synthetic_topics": synthetic_topics_data, # Empty list
            "show_hosts": show_hosts_data, # List of show_host relationships
            "episode_guests": episode_guests_data # List of episode_guest relationships
        }

    except Exception as e:
        log.error(f"An error occurred while extracting data for episode '{feed_entry.get('title', 'Unknown')}': {e}")
        return None
