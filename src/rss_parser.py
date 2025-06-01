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

def _get_show_title(feed, show_data, configured_podcast_name):
    """Determines the show title with fallback logic."""
    title_from_config_value = show_data.get('title')
    title_from_feed = feed.feed.get('title') if feed else None
    
    if title_from_config_value:
        return title_from_config_value
    elif configured_podcast_name:
        return configured_podcast_name
    elif title_from_feed:
        return title_from_feed
    else:
        return 'Unknown Show'


def _get_show_image_url(feed_obj):
    """Extracts image URL from feed object with fallbacks."""
    image_url = ''
    
    # Standard image field
    if 'image' in feed_obj:
        if isinstance(feed_obj['image'], dict):
            image_url = feed_obj['image'].get('href', '')
        elif isinstance(feed_obj['image'], str):
            image_url = feed_obj['image']
    
    # iTunes-specific image as fallback
    if not image_url and 'itunes_image' in feed_obj:
        if isinstance(feed_obj['itunes_image'], dict):
            image_url = feed_obj['itunes_image'].get('href', '')
        elif isinstance(feed_obj['itunes_image'], str):
            image_url = feed_obj['itunes_image']
    
    return image_url


def _extract_show_fields_from_feed(feed, show_data, configured_podcast_name):
    """
    Extracts show-level fields from the feed object.
    
    Args:
        feed: The complete feed object from feedparser
        show_data: Show configuration data from podcasts.json
        configured_podcast_name: The podcast name from configuration
        
    Returns:
        Dictionary containing show fields
    """
    show_title = _get_show_title(feed, show_data, configured_podcast_name)
    
    if feed and hasattr(feed, 'feed'):
        feed_obj = feed.feed
        description = (
            feed_obj.get('subtitle') or 
            feed_obj.get('summary') or 
            feed_obj.get('description') or 
            ''
        )
        image_url = _get_show_image_url(feed_obj)
        website_url = feed_obj.get('link', '')
        language = feed_obj.get('language', '')
        tags = _extract_tags_from_feed(feed_obj)
    else:
        # Fallback to empty values if feed object is not available
        description = ''
        image_url = ''
        website_url = ''
        language = ''
        tags = []
    
    return {
        'title': show_title,
        'description': description,
        'image_url': image_url,
        'website_url': website_url,
        'language': language,
        'tags': tags
    }


def _extract_standard_tags(feed_obj):
    """Extracts tags from standard RSS fields."""
    tags = []
    
    # Standard RSS tags
    if 'tags' in feed_obj:
        tags.extend([tag.get('term', '') for tag in feed_obj['tags'] if tag.get('term')])
    
    # Standard RSS categories
    if 'categories' in feed_obj:
        for cat in feed_obj['categories']:
            if isinstance(cat, dict) and cat.get('term'):
                tags.append(cat['term'])
            elif isinstance(cat, str):
                tags.append(cat)
    
    return tags


def _extract_itunes_categories(feed_obj):
    """Extracts categories from iTunes-specific fields."""
    tags = []
    
    if 'itunes_category' in feed_obj:
        itunes_cats = feed_obj['itunes_category']
        if isinstance(itunes_cats, list):
            for cat in itunes_cats:
                if isinstance(cat, dict) and cat.get('text'):
                    tags.append(cat['text'])
                elif isinstance(cat, str):
                    tags.append(cat)
        elif isinstance(itunes_cats, dict) and itunes_cats.get('text'):
            tags.append(itunes_cats['text'])
    
    return tags


def _extract_tags_from_feed(feed_obj):
    """
    Extracts tags from various sources in the feed object.
    
    Args:
        feed_obj: The feed object from feedparser
        
    Returns:
        List of unique, cleaned tags
    """
    tags = []
    
    # Extract from standard RSS fields
    tags.extend(_extract_standard_tags(feed_obj))
    
    # Extract from iTunes-specific fields
    tags.extend(_extract_itunes_categories(feed_obj))
    
    # Remove duplicates and empty strings
    return list(set([tag.strip() for tag in tags if tag and tag.strip()]))

def extract_episode_data(feed_entry, show_data, configured_podcast_name, feed=None):
    """
    Extracts relevant data from a single feed entry and maps it to BigQuery schema.

    Args:
        feed_entry: A single entry from the parsed feed.
        show_data: A dictionary containing data about the show (from podcasts.json).
        configured_podcast_name: The name of the podcast as per the configuration key.
        feed: The complete feed object (for extracting show-level information).

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
                    break

        if not all([episode_title, published_date_str, audio_url]):
            log.warning(f"Skipping episode due to missing essential data: Title='{episode_title}', Published='{published_date_str}', Audio URL='{audio_url}'")
            return None

        # --- Parse Published Date ---
        try:
            published_date = date_parser.parse(published_date_str)
            if published_date.tzinfo is None:
                log.warning(f"Warning: No timezone info for episode '{episode_title}'. Assuming UTC.")
                published_date = published_date.replace(tzinfo=timezone.utc)
            published_date_utc = published_date.astimezone(timezone.utc)
        except Exception as e:
            log.error(f"Error parsing published date '{published_date_str}' for episode '{episode_title}': {e}")
            return None

        # --- Generate UUIDs ---
        show_id = generate_show_id(configured_podcast_name)
        episode_id = generate_episode_id(show_id, episode_title)
        audio_id = generate_audio_id(episode_id)

        # --- Extract show fields from feed ---
        show_fields = _extract_show_fields_from_feed(feed, show_data, configured_podcast_name)

        # --- Map data to BigQuery Schema ---
        audio_data = {
            "id": audio_id,
            "gcsBucket": None,
            "gcsObjectPath": None,
            "fileSize": None
        }

        show_bq_data = {
            "id": show_id,
            "title": show_fields['title'],
            "sanitizedTitle": utils.sanitize_title(show_fields['title']),
            "description": show_fields['description'],
            "imageUrl": show_fields['image_url'],
            "rssUrl": show_data.get('rss'),
            "websiteUrl": show_fields['website_url'],
            "language": show_fields['language'],
            "tags": show_fields['tags'],
            "lastUpdated": datetime.now(timezone.utc).isoformat()
        }

        episode_description = feed_entry.get('summary', feed_entry.get('description', ''))
        episode_duration_str = feed_entry.get('itunes_duration')
        episode_duration_seconds = parse_duration_to_seconds(episode_duration_str)

        episode_bq_data = {
            "id": episode_id,
            "showId": show_id,
            "title": episode_title,
            "sanitizedTitle": utils.sanitize_title(episode_title),
            "description": episode_description,
            "publishedDate": published_date_utc.isoformat(),
            "durationSeconds": episode_duration_seconds,
            "originalAudioUrl": audio_url,
            "audioId": audio_id
        }


        people_data = []
        show_hosts_data = []
        episode_guests_data = []
        topics_data = []
        synthetic_topics_data = []

        return {
            "audio": audio_data,
            "show": show_bq_data,
            "episode": episode_bq_data,
            "people": people_data,
            "topics": topics_data,
            "synthetic_topics": synthetic_topics_data,
            "show_hosts": show_hosts_data,
            "episode_guests": episode_guests_data
        }

    except Exception as e:
        log.error(f"An error occurred while extracting data for episode '{feed_entry.get('title', 'Unknown')}': {e}")
        return None
