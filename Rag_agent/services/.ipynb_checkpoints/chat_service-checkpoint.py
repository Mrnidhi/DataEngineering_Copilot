import os
from fastapi import FastAPI,HTTPException
from llama_index.llms.ollama import Ollama
# from llama_index.llms.ollama_embedding import OllamaEmbedding  # Assuming this is your embedding model
from llama_index.core.tools import QueryEngineTool, ToolMetadata
from llama_index.core.agent import ReActAgent
from llama_index.core import VectorStoreIndex, Settings
import index
# imports
from llama_index.embeddings.google import GooglePaLMEmbedding


class Agent:
    def __init__(self, directory: str, storage_directory: str):
        self.directory = directory
        self.storage_directory = storage_directory
        self.index = index.Index(directory, storage_directory).load_index()
        if self.index is None:
            raise ValueError("Failed to load or create the index.")
        
        # Initialize query engine with similarity_top_k=3
        self.embedding_name =  "models/embedding-gecko-001"
        api_key="AIzaSyAchKo1r7xttfjHPCpaVpGzd7RyfqbvRdU"
        self.embedding=GooglePaLMEmbedding(model_name=model_name, api_key=api_key)
        embeddings = embed_model.get_text_embedding("Google PaLM Embeddings.")
        self.query_engine = self.index.as_query_engine(
            similarity_top_k=3,
            embedding_model=self.embedding # Use the embedding model
        )
        

        
        # Define metadata for the tool
        self.metadata = ToolMetadata(
            name="QueryEngineTool",
            description=("Handles queries related to Computer Science courses.")
        )
        
        # Initialize the query engine tool with metadata
        self.query_engine_tool = QueryEngineTool(
            query_engine=self.query_engine,
            metadata=self.metadata
        )
        
        # Define ReAct Agent with the query engine tool
        self.llm = Ollama(model="llama3", request_timeout=60.0)
        self.agent = ReActAgent.from_tools([self.query_engine_tool], llm=self.llm, verbose=True)

    def query(self, query: str) -> str:
        if self.index is None:
            raise HTTPException(status_code=500, detail="Index is not loaded.")
        
        # Use the ReAct agent for querying
        response = self.agent.chat(query)
        return response

    def get_react_agent(self) -> ReActAgent:
        return self.agent

async def get_chat_response(message: str) -> str:
    """
    Mocks the processing of a chat message and returns a response.

    Args:
        message (str): The chat message to be processed.

    Returns:
        str: A mock response to the chat message.
    """
    try:
        directory = os.getenv("DATA_STORAGE_DIRECTORY")
        storage_directory = os.getenv("INDEX_STORAGE_DIRECTORY")
        
        # Configure and query the primary agent
        agent = Agent(directory, storage_directory)
        react_agent = agent.get_react_agent()
        
        response = react_agent.chat(message)
        
        return f"This is a mock response to your message: {response}"
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Include if needed later
"""
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8003)
"""
