from environs import Env
import boto3
import botocore

env = Env()
env.read_env()

AWS_ACCESS_KEY_S3 = env.str("AWS_ACCESS_KEY_S3")
AWS_SECRET_KEY_S3 = env.str("AWS_SECRET_KEY_S3")

# Configure your AWS details
BUCKET_NAME = 'arxiv'  # The name of the arXiv bucket
PREFIX = 'pdf/'  # Adjust this prefix based on whether you're downloading PDFs or source files

# Initialize a session using Amazon S3
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_S3,
    aws_secret_access_key=AWS_SECRET_KEY_S3,
    region_name='us-east-1'
)

s3_client = session.client('s3')
s3_resource = session.resource('s3')

# List files in the bucket within the specified prefix
def list_files(bucket_name, prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        for obj in response.get('Contents', []):
            print(obj['Key'])
    except botocore.exceptions.ClientError as e:
        print(e)

# Download a specific file
def download_file(bucket_name, file_key, download_path):
    try:
        object = s3_resource.Bucket(bucket_name).Object(file_key)
        object.download_file(download_path, ExtraArgs={"RequestPayer": "requester"})
        print(f"Downloaded {file_key} to {download_path}")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            print(e)

# Example usage
list_files(BUCKET_NAME, PREFIX)

# Example download
# Replace 'pdf/arXiv_pdf_1001_001.tar' with the actual file key you're interested in
# Adjust the 'download_path' to your specific environment
download_file(BUCKET_NAME, 'pdf/arXiv_pdf_1001_001.tar', 'C:/Users/Nicolai/code/gemini/data/arXiv_pdf_1001_001.tar')