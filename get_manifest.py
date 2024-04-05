from environs import Env
import boto3

env = Env()
env.read_env()

AWS_ACCESS_KEY_S3 = env.str("AWS_ACCESS_KEY_S3")
AWS_SECRET_KEY_S3 = env.str("AWS_SECRET_KEY_S3")

# Initialize a boto3 session

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_S3,
    aws_secret_access_key=AWS_SECRET_KEY_S3,
    region_name='us-east-1'
)

s3 = session.client('s3', region_name='us-east-1')  # arXiv buckets are in the US East (N. Virginia) region

bucket_name = 'arxiv'
manifest_file_key = 'pdf/arXiv_pdf_manifest.xml'  # Adjust this for the correct manifest file
local_file_path = 'C:/Users/Nicolai/code/gemini/data/arXiv_pdf_manifest.xml'  # Local path to save the manifest

# Download the file
s3.download_file(bucket_name, manifest_file_key, local_file_path, ExtraArgs={"RequestPayer": "requester"})
print(f"Downloaded {manifest_file_key} to {local_file_path}")