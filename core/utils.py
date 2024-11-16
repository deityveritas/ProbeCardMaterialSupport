import requests
from urllib3.util.retry import Retry
from urllib.parse import urlparse, urlunparse, ParseResult
from pathlib import Path

from bs4 import BeautifulSoup
import re
from typing import List, Set
import os

from langdetect import detect
from openai import OpenAI

try:
    from presidio_analyzer import AnalyzerEngine
    from presidio_anonymizer import AnonymizerEngine
    analyzer = AnalyzerEngine()
    anonymizer = AnonymizerEngine()
except ImportError:
    print("Presidio is not installed. if PII detection and masking is requested - it will not work.")


img_extensions = ["gif", "jpeg", "jpg", "mp3", "mp4", "png", "svg", "bmp", "eps", "ico"]
doc_extensions = ["doc", "docx", "ppt", "pptx", "xls", "xlsx", "pdf", "ps"]
archive_extensions = ["zip", "gz", "tar", "bz2", "7z", "rar"]
binary_extensions = archive_extensions + img_extensions + doc_extensions

def remove_code_from_html(html_text: str) -> str:
    """Remove code and script tags from HTML."""
    soup = BeautifulSoup(html_text, 'html.parser')
    for tag in soup.find_all(['code', 'script']):
        tag.decompose()
    return str(soup)

def html_to_text(html: str, remove_code: bool = False) -> str:
    """Convert HTML to text."""
    if remove_code:
        html = remove_code_from_html(html)

    # Add spaces before and after list items
    soup = BeautifulSoup(html, features='html.parser')
    for ul in soup.find_all(['ul', 'ol']):
        # Add a space before the list if it directly follows text (e.g., a paragraph)
        prev_sib = ul.find_previous_sibling()
        if prev_sib and prev_sib.name not in ['ul', 'ol', 'li']:  # Avoid double-spacing with adjacent lists
            ul.insert_before(' ')  # Insert a space before the list
        for li in ul.find_all('li'):
            # Insert a space at the beginning of each list item
            li.insert_before(' ')
            # Optionally, insert a space at the end of each list item
            # li.append(' ')

    return soup.get_text()

def create_session_with_retries(retries: int = 3) -> requests.Session:
    """Create a requests session with retries."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        status_forcelist=[429, 500, 502, 503, 504],  # A set of integer HTTP status codes that we should force a retry on.
        backoff_factor=1,
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def remove_anchor(url: str) -> str:
    """Remove the anchor from a URL."""
    parsed = urlparse(url)
    url_without_anchor = urlunparse(parsed._replace(fragment=""))
    return url_without_anchor

def normalize_url(url: str) -> str:
    """Normalize a URL by removing query parameters."""
    # Prepend with 'http://' if URL has no scheme
    if '://' not in url:
        url = 'http://' + url
    p = urlparse(url)

    # Remove query parameters
    path = p.path.split('?', 1)[0]

    # Reconstruct URL with scheme, without 'www', and query parameters
    return ParseResult(p.scheme, p.netloc, path, '', '', '').geturl()

def clean_urls(urls: Set[str]) -> List[str]:
    return list(set(normalize_url(url) for url in urls))

def clean_email_text(text: str) -> str:
    """
    Clean the text email by removing any unnecessary characters and indentation.
    This function can be extended to clean emails in other ways.
    """    
    cleaned_text = text.strip()
    cleaned_text = re.sub(r"[<>]+", "", cleaned_text, flags=re.MULTILINE)
    return cleaned_text

def detect_language(text: str) -> str:
    try:
        lang = detect(text)
        return str(lang)
    except Exception as e:
        print(f"Language detection failed with error: {e}")
        return "en"  # Default to English in case of errors

def get_file_size_in_MB(file_path: str) -> float:
    file_size_bytes = os.path.getsize(file_path)
    file_size_MB = file_size_bytes / (1024 * 1024)    
    return file_size_MB

def get_file_extension(url):
    # Parse the URL to get the path component
    path = urlparse(url).path
    # Use pathlib to extract the file extension
    return Path(path).suffix.lower()


class TableSummarizer():
    def __init__(self, openai_api_key: str):
        self.client = OpenAI(api_key=openai_api_key)

    def summarize_table_text(self, text: str):
        response = self.client.chat.completions.create(
            model="gpt-4-1106-preview",   # GPT4-Turbo
            messages=[
                {"role": "system", "content": "You are a helpful assistant tasked with summarizing tables."},
                {"role": "user", "content": f"Give a concise and comprehensive summary of the table. Table chunk: {text} "},
            ],
            temperature=0
        )
        return response.choices[0].message.content

def mask_pii(text: str) -> str:
    # Analyze and anonymize PII data in the text
    results = analyzer.analyze(
        text=text,
        entities=["PHONE_NUMBER", "CREDIT_CARD", "EMAIL_ADDRESS", "IBAN_CODE", "PERSON", 
                  "US_BANK_NUMBER", "US_PASSPORT", "US_SSN", "LOCATION"],
        language='en')    
    anonymized_text = anonymizer.anonymize(text=text, analyzer_results=results)
    return str(anonymized_text.text)


