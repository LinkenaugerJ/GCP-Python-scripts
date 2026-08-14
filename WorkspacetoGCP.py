import csv
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Replace with the path to your service account key file
SERVICE_ACCOUNT_FILE = '/path/to/service-account-key.json'

# Replace with your GCP project ID and the corresponding IAM and Admin SDK API clients
PROJECT_ID = 'your-project-id'
IAM_API_CLIENT = 'iam'
ADMIN_SDK_API_CLIENT = 'admin'

# Define the columns for the CSV file
CSV_COLUMNS = ['ID', 'First Name', 'Last Name', 'Full Name', 'Primary Email', 'Group Email', 'Role', 'Account Type']

# Define the name of the CSV file
CSV_FILENAME = 'groups.csv'

# Authenticate with the service account
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/admin.directory.user.readonly', 'https://www.googleapis.com/auth/cloud-platform'])

# Build the IAM and Admin SDK API clients
iam_service = build(IAM_API_CLIENT, 'v1', credentials=credentials)
admin_sdk_service = build(ADMIN_SDK_API_CLIENT, 'directory_v1', credentials=credentials)

# Retrieve the list of groups from the GCP project
response = iam_service.projects().getIamPolicy(resource=f'projects/{PROJECT_ID}').execute()
bindings = response.get('bindings', [])

groups = set()
for binding in bindings:
    if binding['role'] == 'roles/iam.serviceAccountUser':
        for member in binding['members']:
            if member.startswith('group:'):
                groups.add(member.split(':')[1])

# Retrieve the list of users from Google Workspace
users = []
page_token = None
while True:
    results = admin_sdk_service.users().list(customer='my_customer', pageToken=page_token).execute()
    users.extend(results.get('users', []))
    page_token = results.get('nextPageToken')
    if not page_token:
        break

# Match the groups to the users and generate the CSV file
with open(CSV_FILENAME, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    for user in users:
        for group in groups:
            if group in user.get('emails', []):
                writer.writerow({
                    'ID': user['id'],
                    'First Name': user.get('name', {}).get('givenName', ''),
                    'Last Name': user.get('name', {}).get('familyName', ''),
                    'Full Name': user.get('name', {}).get('fullName', ''),
                    'Primary Email': user['primaryEmail'],
                    'Group Email': group,
                    'Role': '',
                    'Account Type': user['type']
                })
