import subprocess
import csv
import json
from google.cloud import bigquery

# Set project ID and dataset information
project_id = 'projectName'
dataset_id = 'your_dataset'
table_id = 'your_table'
output_file = 'iam_policy.csv'  # Specify the output file path

# Execute the gcloud command to get the IAM policy
command = f'gcloud projects get-iam-policy {project_id} --format=json'
output = subprocess.check_output(command, shell=True).decode('utf-8')

# Convert the JSON output to a list of dictionaries
policy = json.loads(output)
bindings = policy.get('bindings', [])

# Extract the relevant fields from the bindings
rows = []
for binding in bindings:
    role = binding.get('role', '')
    members = binding.get('members', [])
    for member in members:
        member_type = 'user'  # Default member type
        if member.startswith('serviceAccount:'):
            member_type = 'service account'
            member = member.replace('serviceAccount:', '')
        elif member.startswith('group:'):
            member_type = 'group'
            member = member.replace('group:', '')

        if member_type == 'user' and ':' in member:
            member = member.split(':')[1]

        rows.append({'Role': role, 'Member': member, 'Type': member_type})

# Save the output as a CSV file
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['Role', 'Member', 'Type'])
    writer.writeheader()
    writer.writerows(rows)

print('IAM policy saved as CSV successfully!')

# Initialize BigQuery client
bq_client = bigquery.Client()

# Define BigQuery table reference
table_ref = bq_client.dataset(dataset_id).table(table_id)

# Truncate the table before loading the CSV
truncate_query = f'DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE true'
query_job = bq_client.query(truncate_query)
query_job.result()  # Wait for the query job to complete

# Load the CSV file into BigQuery
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV)
with open(output_file, 'rb') as source_file:
    job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()  # Wait for the job to complete

print('IAM policy inserted into BigQuery successfully!')
