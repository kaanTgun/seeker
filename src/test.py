from google.cloud import bigquery
from google.oauth2 import service_account # If you're using this
from src.logger import setup_logger # Import the custom logger

credentials = service_account.Credentials.from_service_account_file(
    'credentials.json')
client = bigquery.Client(credentials=credentials, project='metapod-d52fc')

# Now, proceed with your query
query_job = client.query("SELECT * FROM `pod_dev01.PEOPLE` LIMIT 10")
results = query_job.result()

log = setup_logger(__name__) # Setup logger for this module
log.info("Test script started")