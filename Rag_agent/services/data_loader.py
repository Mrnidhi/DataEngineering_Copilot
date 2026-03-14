import os
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from llama_index.core import SimpleDirectoryReader, Document
from dotenv import load_dotenv
from langchain_community.document_loaders import WebBaseLoader
from urllib.parse import urljoin, urlparse
from collections import deque

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "dataStorage")

class DataLoader:
    def __init__(self, directory: str = DEFAULT_DATA_DIR):
        self.directory = directory
        print(f"DataLoader directory: {self.directory}")
        # Create directory if it doesn't exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            print(f"Created directory: {self.directory}")
    
    def convert_webpages_to_text_documents(self, urls: list, crawl_depth: int = 1, max_pages: int = 50) -> None:
        """
        Converts webpages to text files with crawling capability.
        
        Args:
            urls: Initial URLs to crawl
            crawl_depth: How deep to crawl (0=only URLs, 1=1 level deep, etc.)
            max_pages: Maximum total pages to crawl
        """
        print(f"Converting webpages with crawl_depth={crawl_depth}, max_pages={max_pages}")
        
        visited = set()
        to_visit = deque([(url, 0) for url in urls])  # (url, depth)
        pages_processed = 0
        
        while to_visit and pages_processed < max_pages:
            current_url, current_depth = to_visit.popleft()
            
            if current_url in visited:
                continue
                
            visited.add(current_url)
            
            try:
                print(f"Processing ({pages_processed+1}/{max_pages}): {current_url}")
                
                # Use WebBaseLoader to fetch content
                loader = WebBaseLoader(web_paths=[current_url])
                docs = loader.load()
                
                text_content = ""
                for doc in docs:
                    if hasattr(doc, 'page_content'):
                        text_content += doc.page_content + "\n"
                    elif hasattr(doc, 'content'):
                        text_content += doc.content + "\n"
                    else:
                        text_content += str(doc) + "\n"
                
                # Clean filename from URL
                parsed = urlparse(current_url)
                path_parts = parsed.path.strip('/').replace('/', '_')
                filename = f"page_{pages_processed+1}_{parsed.netloc}_{path_parts}.txt"
                filename = filename.replace('.html', '').replace('.htm', '')[:100]  # Limit filename length
                
                file_path = os.path.join(self.directory, filename)
                
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(f"URL: {current_url}\n\n{text_content}")
                
                print(f"✅ Saved {len(text_content)} characters to {file_path}")
                pages_processed += 1
                
                # Extract links if we haven't reached max depth
                if current_depth < crawl_depth:
                    soup = BeautifulSoup(text_content, 'html.parser')
                    for link in soup.find_all('a', href=True):
                        absolute_url = urljoin(current_url, link['href'])
                        # Only add links from same domain
                        if urlparse(absolute_url).netloc == urlparse(current_url).netloc:
                            if absolute_url not in visited:
                                to_visit.append((absolute_url, current_depth + 1))
                
            except Exception as e:
                print(f"Error processing {current_url}: {e}")
                continue
        
        print(f"Crawling complete. Processed {pages_processed} pages.")
    
    def load_documents(self, urls: list = None, crawl_depth: int = 1, max_pages: int = 50):
        """
        Load documents from directory, downloading from URLs if provided.
        
        Args:
            urls: URLs to crawl
            crawl_depth: How deep to crawl (0=only URLs, 1=1 level deep, etc.)
            max_pages: Maximum pages to crawl total
        """
        if urls:
            print(f"Converting webpages to text documents...")
            self.convert_webpages_to_text_documents(urls, crawl_depth=crawl_depth, max_pages=max_pages)
        
        if not os.path.exists(self.directory):
            print(f"Directory does not exist: {self.directory}")
            raise Exception(f"Directory does not exist: {self.directory}")
        
        # Check if directory has files
        files = os.listdir(self.directory)
        print(f"Found {len(files)} files in {self.directory}: {files[:5]}...")  # Show first 5 files
        
        if not files:
            print("No files found in directory!")
            return []
        
        try:
            documents = SimpleDirectoryReader(self.directory).load_data()
            print(f"Loaded {len(documents)} documents from directory")
            return documents
        except Exception as e:
            print(f"Error loading documents: {e}")
            return []