import os
import shutil
from typing import List, Optional
from pathlib import Path
from llama_index.core import VectorStoreIndex, StorageContext, SimpleDirectoryReader, Settings
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
import chromadb
from chromadb import PersistentClient
from dotenv import load_dotenv
from data_loader import DataLoader

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent  

DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "dataStorage")
DEFAULT_INDEX_DIR = os.path.join(BASE_DIR, "indexStorage")

class Index:
    def __init__(self, directory: str = DEFAULT_DATA_DIR,
                 storage_directory: str = DEFAULT_INDEX_DIR) -> None:
        self.data_storage_directory = directory
        self.storage_directory = storage_directory
        print(f"Index class constructor: Data storage directory: {self.data_storage_directory}")
        print(f"Index class constructor: Index storage directory: {self.storage_directory}")
        
        # Create directories if they don't exist
        os.makedirs(self.data_storage_directory, exist_ok=True)
        os.makedirs(self.storage_directory, exist_ok=True)
        
        self.index = None
        
        # ALTERNATIVE SOLUTION: Use a smaller, more efficient embedding model
        print("Initializing smaller embedding model for better memory efficiency...")
        self.embedding = HuggingFaceEmbedding(
            # Use a smaller model that's still good quality
            model_name="BAAI/bge-small-en-v1.5",  # 33M params vs 270M for Jina
            embed_batch_size=16,  # Can use larger batch with smaller model
            max_length=512
        )
        Settings.embed_model = self.embedding
        
        # Clean ChromaDB directory if it exists
        chroma_path = os.path.join(self.storage_directory, "chroma.sqlite3")
        if os.path.exists(chroma_path):
            print(f"Removing existing ChromaDB at {chroma_path}...")
            try:
                os.remove(chroma_path)
                for file in os.listdir(self.storage_directory):
                    if file.startswith("chroma"):
                        os.remove(os.path.join(self.storage_directory, file))
            except Exception as e:
                print(f"Warning: Could not remove old ChromaDB files: {e}")
        
        # Initialize Chroma client
        self.chroma_client = PersistentClient(path=self.storage_directory)
        self.collection_name = "rag_index"  

    def create_index(self, documents: List) -> VectorStoreIndex:
        """Creates an index from the provided documents."""
        try:
            print(f"Creating index from {len(documents)} documents...")
            
            # Delete existing collection if it exists
            try:
                self.chroma_client.delete_collection(name=self.collection_name)
                print(f"Deleted existing collection: {self.collection_name}")
            except:
                pass
            
            # Parse documents into nodes
            parser = SemanticSplitterNodeParser(
                embed_model=self.embedding,
                max_chunk_size=768,  # Balanced chunk size
                breakpoint_percentile_threshold=95
            )
            nodes = parser.get_nodes_from_documents(documents)
            print(f"Created {len(nodes)} nodes from documents")
            
            # Create new collection
            chroma_collection = self.chroma_client.create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"}
            )
            
            vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
            storage_context = StorageContext.from_defaults(vector_store=vector_store)
            
            # Create index
            index = VectorStoreIndex(
                nodes,
                storage_context=storage_context,
                embed_model=self.embedding,
                show_progress=True
            )
            
            print("Index created successfully")
            return index
            
        except Exception as e:
            print(f"Error creating index: {e}")
            import traceback
            traceback.print_exc()
            raise

    def load_index(self, urls: list = None, crawl_depth: int = 1, max_pages: int = 50) -> VectorStoreIndex:
        """
        Load existing index or create new one if it doesn't exist.
        """
        try:
            # Try to load existing index
            print("Attempting to load existing index...")
            
            existing_collections = [col.name for col in self.chroma_client.list_collections()]
            print(f"Existing collections: {existing_collections}")
            
            if self.collection_name in existing_collections:
                chroma_collection = self.chroma_client.get_collection(name=self.collection_name)
                
                collection_count = chroma_collection.count()
                print(f"Found {collection_count} documents in existing collection")
                
                if collection_count > 0:
                    print("Loading existing index from vector store...")
                    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
                    storage_context = StorageContext.from_defaults(vector_store=vector_store)
                    index = VectorStoreIndex.from_vector_store(
                        vector_store=vector_store,
                        storage_context=storage_context,
                        embed_model=self.embedding
                    )
                    print("Index loaded from existing storage")
                    return index
            
            print("No existing index found, creating new one...")
                
        except Exception as e:
            print(f"Could not load existing index: {e}")
            
        # Create new index
        try:
            # Load documents with crawling
            print(f"Loading documents from URLs with crawl_depth={crawl_depth}, max_pages={max_pages}")
            data_loader = DataLoader(directory=self.data_storage_directory)
            documents = data_loader.load_documents(urls, crawl_depth=crawl_depth, max_pages=max_pages)
            
            if not documents:
                print("No documents loaded!")
                return None
            
            # Create new index
            index = self.create_index(documents)
            
            print("New index created and ready to use")
            return index
            
        except Exception as create_error:
            print(f"Error creating new index: {create_error}")
            import traceback
            traceback.print_exc()
            return None