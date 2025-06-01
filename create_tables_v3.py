from google.cloud import bigquery
from google.oauth2 import service_account
import json
import re

def clean_sql_statement(statement):
    """Remove SQL comments and clean up the statement"""
    # Remove inline comments (-- style)
    lines = []
    for line in statement.split('\n'):
        # Remove everything after --
        line = re.sub(r'--.*$', '', line)
        if line.strip():
            lines.append(line.strip())
    
    # Join lines and clean up whitespace
    cleaned = ' '.join(lines)
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def main():
    print("Starting table creation process...")

    try:
        # Initialize BigQuery client with credentials
        print("Loading credentials...")
        credentials = service_account.Credentials.from_service_account_file(
            'credentials.json'
        )

        print(f"Connecting to project: {credentials.project_id}")
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # Read the SQL file
        print("Reading SQL file...")
        with open('bq_table.sql', 'r') as f:
            sql_content = f.read()

        # Split into individual statements and clean them
        raw_statements = sql_content.split(';')
        sql_statements = []
        
        print("\nProcessing SQL statements:")
        for stmt in raw_statements:
            cleaned_stmt = clean_sql_statement(stmt)
            if cleaned_stmt:
                print("\nOriginal statement:")
                print(stmt)
                print("\nCleaned statement:")
                print(cleaned_stmt)
                sql_statements.append(cleaned_stmt)

        print(f"Found {len(sql_statements)} SQL statements to execute")

        # Execute each statement
        for i, statement in enumerate(sql_statements, 1):
            try:
                print(f"\nExecuting statement {i}/{len(sql_statements)}...")
                print(f"Statement: {statement}")
                query_job = client.query(statement)
                query_job.result()  # Wait for job to complete
                print(f"Successfully executed statement {i}")
            except Exception as e:
                print(f"\nError executing statement {i}:")
                print(f"Statement:\n{statement}")
                print(f"Error: {str(e)}")

        print("\nTable creation process completed!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
