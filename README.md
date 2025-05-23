# Seeker Podcast Processor

A Google Cloud Function that processes podcast RSS feeds, downloads audio files to Google Cloud Storage, and manages podcast metadata in BigQuery.

## Architecture

- **Cloud Functions (2nd Gen)**: HTTP-triggered function for podcast processing
- **Google Cloud Storage**: Audio file storage
- **BigQuery**: Podcast metadata and episode data storage
- **Firebase Authentication**: Request authentication and authorization

## Prerequisites

1. **Google Cloud Project** with the following APIs enabled:
   - Cloud Functions API
   - Cloud Build API
   - Cloud Run API
   - Cloud Storage API
   - BigQuery API
   - Firebase Authentication API

2. **Required IAM Roles** for deployment:
   - Cloud Functions Admin
   - Cloud Run Admin
   - Cloud Build Service Account
   - Storage Admin
   - BigQuery Admin

3. **Local Development Tools**:
   - Google Cloud SDK (`gcloud` CLI)
   - Python 3.11+
   - Git

## Setup

### 1. Clone and Configure

```bash
git clone <repository-url>
cd seeker
```

### 2. Environment Configuration

Create or update the following configuration files:

#### `.env.deploy` (Deployment Configuration)
```env
PROJECT_ID=your-project-id
SERVICE_NAME=seeker-podcast-processor
REGION=us-central1
MEMORY=2Gi
CPU=2
TIMEOUT=3600
CONCURRENCY=1000
MAX_INSTANCES=5
PLATFORM=linux/amd64
```

#### `.env` (Runtime Environment Variables)
```env
GCP_PROJECT_ID="your-project-id"
GCS_BUCKET_NAME="your-bucket-name"
BIGQUERY_DATASET_ID="your-dataset-id"
```

#### `config/podcasts.json` (Podcast Configuration)
```json
{
    "All In": {
        "rss": "https://allinchamathjason.libsyn.com/rss"
    },
    "Lex Fridman": {
        "rss": "https://lexfridman.com/feed/podcast/"
    }
}
```

### 3. Service Account Setup

1. Create a service account for the Cloud Function:
```bash
gcloud iam service-accounts create seeker-podcast-processor \
    --display-name="Seeker Podcast Processor"
```

2. Grant necessary permissions:
```bash
# BigQuery permissions
gcloud projects add-iam-policy-binding your-project-id \
    --member="serviceAccount:seeker-podcast-processor@your-project-id.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

# Cloud Storage permissions
gcloud projects add-iam-policy-binding your-project-id \
    --member="serviceAccount:seeker-podcast-processor@your-project-id.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Firebase Admin permissions
gcloud projects add-iam-policy-binding your-project-id \
    --member="serviceAccount:seeker-podcast-processor@your-project-id.iam.gserviceaccount.com" \
    --role="roles/firebase.sdkAdminServiceAgent"
```

### 4. Cloud Build Service Account Permissions

Grant Cloud Build service account the necessary permissions to deploy Cloud Functions:

```bash
# Get your project number
PROJECT_NUMBER=$(gcloud projects describe your-project-id --format="value(projectNumber)")

# Grant Cloud Run Admin role to Cloud Build service account
gcloud projects add-iam-policy-binding your-project-id \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/run.admin"
```

### 5. BigQuery Dataset and Tables Setup

Create the required BigQuery dataset and tables:

```bash
# Create dataset
bq mk --dataset your-project-id:your-dataset-id

# Create tables (schema definitions needed - see BigQuery Schema section)
```

## Deployment

### Method 1: Cloud Build (Recommended)

1. **Authenticate with Google Cloud:**
```bash
gcloud auth login
gcloud config set project your-project-id
```

2. **Deploy using Cloud Build:**
```bash
gcloud builds submit --config=cloudbuild.yaml
```

### Method 2: Direct gcloud Deployment

```bash
gcloud functions deploy seeker-podcast-processor \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=. \
    --entry-point=cloud_function_entrypoint \
    --trigger-http \
    --allow-unauthenticated \
    --memory=2Gi \
    --timeout=3600s \
    --max-instances=5 \
    --concurrency=1000
```

## Deployment Verification

### 1. Check Function Status
```bash
gcloud functions describe seeker-podcast-processor \
    --region=us-central1 \
    --format="value(state,url)"
```

### 2. Test Function Endpoint
```bash
# Test authentication (should return 401)
curl -X POST https://us-central1-your-project-id.cloudfunctions.net/seeker-podcast-processor \
    -H "Content-Type: application/json" \
    -d '{"podcast_name": "All In", "num_episodes": 1}'

# Expected response: {"message":"Authorization header missing."}
```

### 3. Check Logs
```bash
gcloud functions logs read seeker-podcast-processor \
    --region=us-central1 \
    --limit=10
```

## Usage

### API Request Format

**Endpoint:** `https://us-central1-your-project-id.cloudfunctions.net/seeker-podcast-processor`

**Method:** POST

**Headers:**
```
Content-Type: application/json
Authorization: Bearer <firebase-id-token>
```

**Request Body:**
```json
{
    "podcast_name": "All In",
    "num_episodes": 2
}
```

**Response (Success):**
```json
{
    "message": "Successfully processed 2 episodes for 'All In'."
}
```

### Authentication

The function requires Firebase Authentication in production. To bypass authentication for local testing:

1. The function automatically detects local vs. deployed environment
2. Authentication is skipped when `K_SERVICE` environment variable is not present

## Local Development

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Up Local Environment
```bash
# Set up Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"

# Set environment variables
export GCP_PROJECT_ID="your-project-id"
export GCS_BUCKET_NAME="your-bucket-name"
export BIGQUERY_DATASET_ID="your-dataset-id"
```

### 3. Run Local Tests
```bash
cd src
python main.py
```

## Troubleshooting

### Common Issues

1. **Import Errors:**
   - Ensure all imports use absolute paths (`from src.module import ...`)
   - Check that all required dependencies are in `requirements.txt`

2. **Permission Errors:**
   - Verify Cloud Build service account has `roles/run.admin`
   - Check that function service account has BigQuery and Storage permissions

3. **Container Startup Failures:**
   - Check that `--entry-point=cloud_function_entrypoint` is specified
   - Verify the function listens on `PORT=8080`

4. **Authentication Issues:**
   - Ensure Firebase project is properly configured
   - Verify service account has Firebase admin permissions

### Logs and Monitoring

**View Function Logs:**
```bash
gcloud functions logs read seeker-podcast-processor --region=us-central1
```

**View Build Logs:**
```bash
gcloud builds list --limit=5
gcloud builds log <build-id>
```

**Monitor Cloud Run Service:**
```bash
gcloud run services describe seeker-podcast-processor --region=us-central1
```

## BigQuery Schema

The function expects the following BigQuery tables in your dataset:

### SHOWS Table
- `id` (STRING)
- `title` (STRING)
- `sanitizedTitle` (STRING)
- `description` (STRING)
- `imageUrl` (STRING)
- `rssUrl` (STRING)
- `websiteUrl` (STRING)
- `language` (STRING)
- `tags` (STRING, REPEATED)
- `lastUpdated` (TIMESTAMP)

### EPISODES Table
- `id` (STRING)
- `showId` (STRING)
- `title` (STRING)
- `sanitizedTitle` (STRING)
- `description` (STRING)
- `publishedDate` (TIMESTAMP)
- `durationSeconds` (INTEGER)
- `originalAudioUrl` (STRING)
- `audioId` (STRING)

### AUDIO Table
- `id` (STRING)
- `gcsBucket` (STRING)
- `gcsObjectPath` (STRING)
- `fileSize` (INTEGER)

### PEOPLE Table
- `id` (STRING)
- `full_name` (STRING)
- `aliases` (STRING)
- `audioId` (STRING)

## Project Structure

```
seeker/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── cloudbuild.yaml          # Cloud Build configuration
├── main.py                  # Root entry point
├── .env                     # Runtime environment variables
├── .env.deploy              # Deployment configuration
├── config/
│   └── podcasts.json        # Podcast RSS configurations
└── src/
    ├── main.py              # Main function implementation
    ├── auth_handler.py      # Firebase authentication
    ├── rss_parser.py        # RSS feed parsing
    ├── gcs_handler.py       # Google Cloud Storage operations
    ├── bq_handler.py        # BigQuery operations
    ├── logger.py            # Logging configuration
    └── utils.py             # Utility functions
```

## Contributing

1. Follow the existing code structure and import patterns
2. Update `requirements.txt` for new dependencies
3. Test locally before deploying
4. Update this README for any configuration changes

## License

[Your License Here]
