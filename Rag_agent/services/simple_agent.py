import os
import asyncio
from pathlib import Path
from llama_index.llms.ollama import Ollama
from llama_index.core.tools import QueryEngineTool, ToolMetadata
from llama_index.core.agent import ReActAgent
from llama_index.core import Settings
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from dotenv import load_dotenv

from index import Index
from chat_memory import ChatMemory

load_dotenv()

class SimpleWorkingAgent:
    def __init__(self, directory: str, storage_directory: str, user_id: str):
        self.directory = directory
        self.storage_directory = storage_directory
        self.user_id = user_id
        
        # Initialize chat memory
        try:
            self.chat_memory = ChatMemory(user_id=user_id)
        except Exception as e:
            print(f"Note: Chat memory initialization failed ({e}), continuing without it")
            self.chat_memory = None
            
        # Target Documentation URLs
        self.urls = [
            "https://spark.apache.org/docs/latest/sql-getting-started.html",
            "https://spark.apache.org/docs/latest/sql-programming-guide.html",
            "https://docs.getdbt.com/reference/references-overview",
            "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html"
        ]
        
        print(f"Loading index from {storage_directory}...")
        self.index = Index(directory, storage_directory).load_index(urls=self.urls)
        if not self.index:
            raise ValueError("Failed to create index")
            
        self._setup_models()
        self._setup_agent()
        
    def _setup_models(self) -> None:
        """Configures the embedding and LLM models using environment variables."""
        print("Setting up models...")
        
        # Embeddings
        embedding = HuggingFaceEmbedding(
            model_name="BAAI/bge-small-en-v1.5",
            model_kwargs={"attn_implementation": "eager"}
        )
        
        # LLM from ENV
        ollama_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        ollama_model = os.getenv("OLLAMA_MODEL", "llama3.2:3b-instruct-q4_K_S")
        
        llm = Ollama(
            model=ollama_model, 
            base_url=ollama_url,
            request_timeout=120.0, 
            temperature=0.0  # Set to 0 for deterministic outputs
        )
        
        Settings.embed_model = embedding
        Settings.llm = llm

    def _setup_agent(self) -> None:
        """Configures the Chat Engine and ReAct Agent framework."""
        mem_slots = self.chat_memory.get_all() if self.chat_memory else []
        
        self.chat_engine = self.index.as_chat_engine(
            similarity_top_k=3,
            response_mode="compact",
            chat_memory=mem_slots
        )
        
        tool = QueryEngineTool(
            query_engine=self.chat_engine,
            metadata=ToolMetadata(
                name="docs_search",
                description="Search ONLY in indexed Apache Spark, dbt, and Apache Airflow documentation. NEVER answer about Kafka, Snowflake, or other technologies."
            )
        )
        
        system_prompt = """
            You are a conversational documentation assistant for a Data Engineering Copilot.

            You have access to:
            - Documentation search tool (Spark, dbt, Airflow)
            - Conversation history provided in the prompt

            Rules:
            1. Treat this as an ongoing conversation, not a single question
            2. Use prior Q&A to understand follow-up questions
            3. Use docs_search when factual grounding is required
            4. If the documentation does not contain the answer, say so clearly
            5. Do NOT use general knowledge outside the indexed documents
            """
            
        self.agent = ReActAgent.from_tools(
            tools=[tool],
            llm=Settings.llm,
            verbose=True,
            max_iterations=2,
            context=system_prompt
        )
        print("ReActAgent initialized successfully.")
    
    def save_to_memory(self, question: str, answer: str):
        """Save Q&A to memory, handling serialization issues"""
        if self.chat_memory:
            try:
                # Convert to simple strings to avoid serialization issues
                self.chat_memory.put_messages({
                    "question": str(question),
                    "answer": str(answer)
                })
            except Exception as e:
                print(f"Note: Could not save to chat memory: {e}")
    
    def query(self, question: str) -> str:
        """
        Simple sync wrapper for async ReActAgent
        """
        # Pre-check for explicitly unsupported topics
        unsupported_topics = ['kafka', 'snowflake', 'mongodb', 'redis', 'elasticsearch', 'postgres', 'mysql']
        question_lower = question.lower()
        
        for topic in unsupported_topics:
            if topic in question_lower:
                return f"I don't have {topic.capitalize()} documentation. I can only help with Apache Spark, dbt, and Apache Airflow based on the documents I have indexed."
        
        async def run_agent():
            try:
                print(f"\n=== Processing: {question} ===")

                # 🔹 STEP 3: Run agent with memory-aware prompt
                response = self.chat_engine.chat(question)

                if hasattr(response, 'response'):
                    result = str(response.response)
                elif hasattr(response, 'output'):
                    result = str(response.output)
                elif hasattr(response, 'content'):
                    result = str(response.content)
                else:
                    result = str(response)

                return result

                    
            except Exception as e:
                print(f"Agent error: {e}")
                # Don't fallback to general knowledge
                return "I encountered an error searching the documentation. Please rephrase your question about Spark, dbt, or Airflow."
        
        # Run the async function synchronously
        result = asyncio.run(run_agent())
        
        # Save to chat memory (handling serialization)
        self.save_to_memory(question, result)
        
        return result

