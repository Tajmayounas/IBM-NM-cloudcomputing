# IBM-NM-cloudcomputing
from google.cloud import bigquery

# Replace 'your-project-id' and 'your-dataset-id' with your actual project and dataset IDs
project_id = 'your-project-id'
dataset_id = 'your-dataset-id'

# Initialize a BigQuery client
client = bigquery.Client(project=project_id)

# Example: Create a new dataset if it doesn't exist
dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

try:
    client.get_dataset(dataset)
    print(f'Dataset {dataset_id} already exists.')
except Exception as e:
    print(f'Dataset {dataset_id} does not exist. Creating...')
    client.create_dataset(dataset)
    print(f'Dataset {dataset_id} created successfully.')

# Example: Load data into a BigQuery table
table_id = 'your-table-id'
table_ref = dataset_ref.table(table_id)
table = bigquery.Table(table_ref)

# Replace 'your-data.csv' with the path to your actual data file
data_file_path = 'your-data.csv'

# Specify job configuration (load job)
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

with open(data_file_path, 'rb') as source_file:
    job = client.load_table_from_file(source_file, table, job_config=job_config)

# Wait for the job to complete
job.result()

print(f'Data loaded into table {table_id} successfully.')
