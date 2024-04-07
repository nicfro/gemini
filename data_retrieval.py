from environs import Env
import boto3
import botocore
import re
import requests
import time
from xml.etree import ElementTree
from collections import defaultdict

class DataRetrieval:

    def __init__(self, category='cs.LG', max_results=20):
        env = Env()
        env.read_env()

        # AWS_ACCESS_KEY_S3 = env.str("AWS_ACCESS_KEY_S3")
        # AWS_SECRET_KEY_S3 = env.str("AWS_SECRET_KEY_S3")

        # Configure your AWS details
        self.bucket_name = 'arxiv'  # The name of the arXiv bucket
        self.prefix = 'pdf/'  # Adjust this prefix based on whether you're downloading PDFs or source files

        # Initialize a session using Amazon S3
        # self.session = boto3.Session(
        #     aws_access_key_id=AWS_ACCESS_KEY_S3,
        #     aws_secret_access_key=AWS_SECRET_KEY_S3,
        #     region_name='us-east-1'
        # )
        # self.s3_client = self.session.client('s3')
        # self.s3_resource = self.session.resource('s3')
        self.category = category
        self.max_results = max_results

    def get_manifest(self, file_path:str):
        manifest_file_key = 'pdf/arXiv_pdf_manifest.xml'  # Adjust this for the correct manifest file
        local_file_path = file_path  # Local path to save the manifest

        # Download the file
        self.s3_client.download_file(self.bucket_name, manifest_file_key, local_file_path, ExtraArgs={"RequestPayer": "requester"})
        print(f"Downloaded {manifest_file_key} to {local_file_path}")

    def fetch_metadata(self, start=0, minimum=100) -> ElementTree.Element:
        count = self.max_results
        all_metadata = self.fetch_metadata_category_articles(start)

        while count < minimum:
            time.sleep(1)
            metadata_xml = self.fetch_metadata_category_articles(count)
            all_metadata.extend(metadata_xml.findall('{http://www.w3.org/2005/Atom}entry'))
            count += self.max_results
        return all_metadata

    # Fetch metadata for articles in a specific category
    def fetch_metadata_category_articles(self, start=0) -> ElementTree.Element:
        ARXIV_API_URL = "http://export.arxiv.org/api/query?"
        query_params = {
            "search_query": f"cat:{self.category}",
            "start": start,
            "max_results": self.max_results
        }

        response = requests.get(ARXIV_API_URL, params=query_params)
        if response.status_code == 200:
            return ElementTree.fromstring(response.content)
        else:
            raise Exception(f"Failed to fetch data: HTTP Status Code {response.status_code}")
        
    # Save metadata for articles in a specific category to file
    def save_metadata_to_file(self, metadata:ElementTree.Element, file_path:str):
        with open(file_path, 'wb') as f:
            f.write(ElementTree.tostring(metadata))

    def load_metadata_from_file(self, file_path:str) -> ElementTree.Element:
        with open(file_path, 'rb') as f:
            return ElementTree.parse(f)

    def normalize_article_id(self, article_id:str) -> str:
        # Remove the version number
        article_id_without_version = re.sub(r'v\d+$', '', article_id)
        # Remove any dots
        normalized_id = article_id_without_version.replace('.', '')
        return normalized_id
    
    def get_normalized_article_id_from_metadata(self, metadata:ElementTree.Element) -> list:
        ids = []
        for entry in metadata.findall('{http://www.w3.org/2005/Atom}entry'):
            id_text = entry.find('{http://www.w3.org/2005/Atom}id').text
            article_id = id_text.split('/')[-1]  # Extracts the arXiv ID from the full URL

            ids.append(self.normalize_article_id(article_id))
        return ids

    # Print metadata for articles
    def print_metadata(self, metadata:ElementTree.Element):
        for entry in metadata.findall('{http://www.w3.org/2005/Atom}entry'):
            title = entry.find('{http://www.w3.org/2005/Atom}title').text.strip()
            authors = [author.find('{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')]
            id_text = entry.find('{http://www.w3.org/2005/Atom}id').text
            article_id = id_text.split('/')[-1]  # Extracts the arXiv ID from the full URL
            journal_ref = entry.find('{http://arxiv.org/schemas/atom}journal_ref').text if entry.find('{http://arxiv.org/schemas/atom}journal_ref') is not None else "No journal reference available"
            categories = entry.find('{http://www.w3.org/2005/Atom}category').attrib['term'] if entry.find('{http://www.w3.org/2005/Atom}category') is not None else "No categories available"
            abstract = entry.find('{http://www.w3.org/2005/Atom}summary').text.strip() if entry.find('{http://www.w3.org/2005/Atom}summary') is not None else "No abstract available"

            print(f"Article ID: {article_id}")
            print(f"Normalized ID: {self.normalize_article_id(article_id)}")
            print(f"Categories: {categories}")
            print(f"Article Identifier: {categories}-{article_id}")
            print(f"Title: {title}")
            print(f"Authors: {', '.join(authors)}")
            print(f"Journal Reference: {journal_ref}")
            print(f"Abstract: {abstract}")
            print("--------------------------------------------------")

    def download_file(self, file_key:str, download_path:str):
        try:
            object = self.s3_resource.Bucket(self.bucket_name).Object(file_key)
            object.download_file(download_path, ExtraArgs={"RequestPayer": "requester"})
            print(f"Downloaded {file_key} to {download_path}")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                print(e)

    def search_manifest(self, article_id:str, manifest_file_path:str):
        tree = ElementTree.parse(manifest_file_path)
        root = tree.getroot()

        def remove_non_numeric_chars(s):
            return int(re.sub(r'[^0-9]', '', s))

        article_id = int(self.normalize_article_id(article_id))
        for file in root.findall('file'):
            first_item = remove_non_numeric_chars(file.find('first_item').text)
            last_item = remove_non_numeric_chars(file.find('last_item').text)
            if first_item <= article_id <= last_item:
                return file.find('filename').text
        return None
    
    def find_files_in_manifest_from_metadata(self, metadata:str, manifest_file_path:str):
        metadata = self.load_metadata_from_file(metadata)
        ids = self.get_normalized_article_id_from_metadata(metadata)
        files = defaultdict(int)
        for id in ids:
            filename = self.search_manifest(id, manifest_file_path)
            if filename:
                files[filename] += 1
        return files
        
retrieve = DataRetrieval('cs.LG', 20)
all_metadata = retrieve.fetch_metadata(0, 200)
retrieve.save_metadata_to_file(all_metadata, 'C:\\Users\\jooda\\Documents\\GitHub\\gemini\\data\\metadata.xml')
files = retrieve.find_files_in_manifest_from_metadata('C:\\Users\\jooda\\Documents\\GitHub\\gemini\\data\\metadata.xml', 'C:\\Users\\jooda\\Documents\\GitHub\\gemini\\data\\arXiv_pdf_manifest.xml')
print(files)
