import xml.etree.ElementTree as ET
import re

def normalize_article_id(article_id):
    # Remove the version number
    article_id_without_version = re.sub(r'v\d+$', '', article_id)
    # Remove any dots
    normalized_id = article_id_without_version.replace('.', '')
    return int(normalized_id)

def remove_non_numeric_chars(s):
    return int(re.sub(r'[^0-9]', '', s))

# Load and parse the manifest file
manifest_file_path = 'C:\\Users\\Nicolai\\code\\gemini\\data\\arXiv_pdf_manifest.xml'  # Update this path
tree = ET.parse(manifest_file_path)
root = tree.getroot()

def find_file_in_manifest(article_id):
    article_id = normalize_article_id(article_id)
    for file in root.findall('file'):
        first_item = remove_non_numeric_chars(file.find('first_item').text)
        last_item = remove_non_numeric_chars(file.find('last_item').text)
        if first_item <= article_id <= last_item:
            return file.find('filename').text
    return None

# Example usage 

article_id = '08074198'  # Update this to the article ID you're looking for
filename = find_file_in_manifest(article_id)
if filename:
    print(f"The article {article_id} is in {filename}.")
else:
    print(f"Article {article_id} was not found in the manifest.")