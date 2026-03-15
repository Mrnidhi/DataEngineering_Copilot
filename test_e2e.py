import sys
import os
import time

# Add required paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, 'Rag_agent', 'services'))

from app.api.endpoints.airflow import AirflowClient
from chat_service import analyze_failure

def test_e2e():
    print("Initializing Airflow Client...")
    client = AirflowClient()
    
    print("\nWaiting for DAGs to complete (checking for failures)...")
    failures = []
    
    # Poll for 60 seconds
    for _ in range(12):
        print("Polling active failures...")
        failures = client.get_active_failures()
        if failures:
            break
        time.sleep(5)
        
    if not failures:
        print("❌ No failures detected in Airflow yet. The DAG might still be running or it passed.")
        
        # Print run states
        print("Current DAG runs:")
        print(client.get_recent_dag_runs("failing_dbt_pipeline"))
        print(client.get_recent_dag_runs("successful_elt_pipeline"))
        return

    failure = failures[0]
    print(f"\n✅ Anomaly Detected: DAG `{failure['dag_id']}`, Task `{failure['task_id']}`")
    print(f"Log snippet length: {len(failure['logs'])}")
    
    print("\nStarting AI Diagnosis (This requires Llama to process the RAG index)...")
    start_time = time.time()
    diagnosis = analyze_failure(failure['dag_id'], failure['task_id'], failure['logs'])
    end_time = time.time()
    
    print(f"\n======== 🤖 AI ROOT CAUSE DIAGNOSIS ========")
    print(diagnosis)
    print("============================================")
    print(f"\nDiagnosis took {end_time - start_time:.2f} seconds.")
    print("\n✅ End-to-End Test Completed Successfully!")

if __name__ == "__main__":
    test_e2e()
