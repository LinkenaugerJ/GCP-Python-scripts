import csv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime

# Set your Google Cloud project ID
development_project = "your-project-id"  # Replace with your valid project ID

# Initialize the BigQuery client
client = bigquery.Client(project=development_project)

# Define a function to process datasets and tables
def process_datasets_and_tables():
    # Get a reference to the output BigQuery table
    output_dataset = "TableAuditResults"
    output_table = "table_audit_results"

    # Initialize an empty list to store the collected data
    collected_data = []

    # Get a list of datasets and filter them
    filtered_datasets = datasetFilter(client.list_datasets())

    dataset_count = len(filtered_datasets)
    print(f"Processing {dataset_count} datasets...")

    for i, dataset in enumerate(filtered_datasets, start=1):
        # Loop through tables within the dataset
        print(f"Processing dataset {i} of {dataset_count}: {dataset.dataset_id}")
        for table in client.list_tables(dataset.dataset_id):
            try:
                # Fetch additional details for each table
                full_table = client.get_table(table)

                # Create a Python dictionary representing your data
                current_datetime = datetime.now()
                data = [
                    development_project,
                    full_table.dataset_id,
                    full_table.table_id,
                    full_table.created,
                    full_table.modified,
                    full_table.expires,
                    full_table.num_rows,
                    full_table.num_bytes / (1024 * 1024),
                    full_table.partition_expiration,
                    full_table.partitioning_type,
                    full_table.clustering_fields,
                    current_datetime
                ]

                collected_data.append(data)
                
            except NotFound:
                # Handle the case where the table is not found
                print(f"Table not found: {table.table_id}")

    # Specify the path for the CSV file
    csv_file_path = "output_data.csv"

    # Write the data to a CSV file
    with open(csv_file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "project_id", "dataset_id", "table_id", "created_date", "last_modified",
            "default_table_expiration", "row_count", "size_mb", "partition_expiration",
            "partition_column", "clustering_fields", "insert_date"
        ])
        writer.writerows(collected_data)

    print(f"Processed {len(collected_data)} datasets and tables. Data saved to CSV.")

# Rest of the code (including the datasetFilter function) remains the same
