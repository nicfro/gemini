from google.cloud import storage

# 1. Download google credentials file. 
# 2. export GOOGLE_APPLICATION_CREDENTIALS="./model-factor-419419-293ee85911c1.json" or whatever the path is
# 3. Call this function to upload the doc

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

bucket_name = 'papers-gemini'
source_file_name = './data/arXiv_pdf_manifest.xml'
destination_blob_name = 'manifest'

upload_blob(bucket_name, source_file_name, destination_blob_name)