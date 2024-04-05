import requests
from xml.etree import ElementTree
import re

def normalize_article_id(article_id):
    # Remove the version number
    article_id_without_version = re.sub(r'v\d+$', '', article_id)
    # Remove any dots
    normalized_id = article_id_without_version.replace('.', '')
    return normalized_id

def fetch_arxiv_metadata(category="cs.LG", start=0, max_results=5):
    ARXIV_API_URL = "http://export.arxiv.org/api/query?"
    query_params = {
        "search_query": f"cat:{category}",
        "start": start,
        "max_results": max_results
    }
    
    response = requests.get(ARXIV_API_URL, params=query_params)
    if response.status_code == 200:
        return ElementTree.fromstring(response.content)
    else:
        raise Exception(f"Failed to fetch data: HTTP Status Code {response.status_code}")

# Example usage to fetch the first 10 entries in the cs.LG category
metadata_xml = fetch_arxiv_metadata("cs.LG", 20, 70)

for entry in metadata_xml.findall('{http://www.w3.org/2005/Atom}entry'):
    title = entry.find('{http://www.w3.org/2005/Atom}title').text.strip()
    authors = [author.find('{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')]
    id_text = entry.find('{http://www.w3.org/2005/Atom}id').text
    article_id = id_text.split('/')[-1]  # Extracts the arXiv ID from the full URL
    journal_ref = entry.find('{http://arxiv.org/schemas/atom}journal_ref').text if entry.find('{http://arxiv.org/schemas/atom}journal_ref') is not None else "No journal reference available"
    categories = entry.find('{http://www.w3.org/2005/Atom}category').attrib['term'] if entry.find('{http://www.w3.org/2005/Atom}category') is not None else "No categories available"
    abstract = entry.find('{http://www.w3.org/2005/Atom}summary').text.strip() if entry.find('{http://www.w3.org/2005/Atom}summary') is not None else "No abstract available"
    
    print(f"Article ID: {article_id}")
    print(normalize_article_id(article_id))
    print(f"Categories: {categories}")
    print(f"Article Identifier: {categories}-{article_id}")
    print(f"Title: {title}")
    print(f"Authors: {', '.join(authors)}")
    print(f"Journal Reference: {journal_ref}")
    print(f"Abstract: {abstract}")
    print("--------------------------------------------------")
