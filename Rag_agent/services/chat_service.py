import os
from pathlib import Path
from simple_agent import SimpleWorkingAgent

# Cache for agents by user_id
_agent_cache = {}

def get_query_response(query: str, user_id: str, clear_history: bool = False) -> str:
    """
    Get response from the agent for a given query.
    
    Args:
        query: The user's question
        user_id: User identifier for chat history
        clear_history: Whether to clear chat history
    
    Returns:
        Agent's response as string
    """
    try:
        # Set up paths
        BASE_DIR = Path(__file__).resolve().parent.parent
        data_dir = str(BASE_DIR / "dataStorage")
        storage_dir = str(BASE_DIR / "indexStorage")
        
        # Get or create agent for this user
        if user_id not in _agent_cache:
            print(f"Creating new agent for user: {user_id}")
            _agent_cache[user_id] = SimpleWorkingAgent(data_dir, storage_dir, user_id)
        
        agent = _agent_cache[user_id]
        
        # Clear history if requested
        if clear_history:
            agent.chat_memory.clear_history()
        
        # Get response
        response = agent.query(query)
        
        return response
        
    except Exception as e:
        print(f"Error in chat_service: {e}")
        return f"Error: {str(e)}"

def analyze_failure(dag_id: str, task_id: str, logs: str, user_id: str = "monitoring_system") -> str:
    """
    Feeds specific Airflow task failure logs to the RAG Agent to diagnose based on docs.
    """
    try:
        # We only really need the bottom part of the traceback usually
        # But we'll grab the last 2000 chars to avoid overwhelming the context window
        truncated_logs = logs[-2000:] if len(logs) > 2000 else logs
        
        prompt = f"""
        🚨 AIRFLOW TASK FAILURE DETECTED 🚨
        
        DAG: {dag_id}
        Task: {task_id}
        
        Logs snippet:
        ```text
        {truncated_logs}
        ```
        
        Using ONLY your knowledge of Apache Spark, dbt, and Apache Airflow documentation:
        1. Identify the exact root cause of this error.
        2. Explain why it happened based on the documentation.
        3. Provide the specific code or configuration change needed to fix it.
        
        Keep your response actionable and focused on the fix. Do not summarize the entire log.
        """
        
        # We use the existing query function to invoke the ReAct agent
        # with our highly specific troubleshooting prompt
        response = get_query_response(prompt, user_id, clear_history=True)
        return response
    
    except Exception as e:
        print(f"Error analyzing failure logs: {e}")
        return "Could not generate AI diagnostic for this failure."