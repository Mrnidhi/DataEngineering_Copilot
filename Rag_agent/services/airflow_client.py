import os
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import urllib.parse
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class AirflowClient:
    def __init__(self):
        self.base_url = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
        username = os.getenv("AIRFLOW_USERNAME", "airflow")
        password = os.getenv("AIRFLOW_PASSWORD", "airflow")
        self.auth = HTTPBasicAuth(username, password)
        self.headers = {"Content-Type": "application/json"}

    def _get(self, endpoint: str) -> Dict[str, Any]:
        """Make a GET request to the Airflow API."""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, auth=self.auth, headers=self.headers, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError:
            print(f"Airflow connection refused at {url}")
            return {}
        except requests.exceptions.Timeout:
            print(f"Airflow connection timeout at {url}")
            return {}
        except requests.exceptions.RequestException as e:
            print(f"Error accessing Airflow API ({url}): {e}")
            return {}

    def get_dags(self) -> List[Dict[str, Any]]:
        """Retrieve all DAGs."""
        data = self._get("dags")
        return data.get("dags", [])

    def get_recent_dag_runs(self, dag_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Retrieve recent runs for a given DAG."""
        data = self._get(f"dags/{dag_id}/dagRuns?order_by=-execution_date&limit={limit}")
        return data.get("dag_runs", [])

    def get_failed_task_instances(self, dag_id: str, dag_run_id: str) -> List[Dict[str, Any]]:
        """Retrieve task instances for a specific DAG run that have failed."""
        encoded_dag_run_id = urllib.parse.quote(dag_run_id)
        data = self._get(f"dags/{dag_id}/dagRuns/{encoded_dag_run_id}/taskInstances?state=failed")
        return data.get("task_instances", [])

    def get_task_logs(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
        """Retrieve logs for a specific task instance."""
        encoded_dag_run_id = urllib.parse.quote(dag_run_id)
        
        # Text/plain log request
        url = f"{self.base_url}/dags/{dag_id}/dagRuns/{encoded_dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        try:
            response = requests.get(url, auth=self.auth, headers={"Accept": "text/plain"})
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching logs ({url}): {e}")
            return "Could not retrieve logs."

    def get_active_failures(self) -> List[Dict[str, Any]]:
        """
        Scans all DAGs for recent runs and returns details of any that are currently failing.
        This will be the main entry point for our monitoring UI.
        """
        all_dags = self.get_dags()
        failures = []
        
        for dag in all_dags:
            if not dag.get("is_active") or dag.get("is_paused"):
                continue
                
            dag_id = dag["dag_id"]
            recent_runs = self.get_recent_dag_runs(dag_id, limit=1)
            
            if not recent_runs:
                continue
                
            latest_run = recent_runs[0]
            if latest_run.get("state") == "failed":
                # Find exactly which task failed
                failed_tasks = self.get_failed_task_instances(dag_id, latest_run["dag_run_id"])
                
                for task in failed_tasks:
                    task_id = task["task_id"]
                    try_number = task.get("try_number", 1)
                    logs = self.get_task_logs(dag_id, latest_run["dag_run_id"], task_id, try_number)
                    
                    failures.append({
                        "dag_id": dag_id,
                        "run_id": latest_run["dag_run_id"],
                        "task_id": task_id,
                        "execution_date": latest_run.get("execution_date"),
                        "logs": logs,
                        "status": "failed"
                    })
                    
        return failures

    def get_dashboard_summary(self) -> List[Dict[str, Any]]:
        """Returns a summary of the latest run status for all active DAGs."""
        summary = []
        dags = self.get_dags()
        
        for dag in dags:
            if dag.get("is_paused") or dag["dag_id"] == "dataset_consumes_1":
                continue # Skip paused dags and internal airflow dags
                
            dag_id = dag["dag_id"]
            runs = self.get_recent_dag_runs(dag_id, limit=1)
            
            status = "unknown"
            execution_date = "N/A"
            run_id = None
            
            if runs:
                status = runs[0].get("state", "unknown")
                execution_date = runs[0].get("execution_date", "N/A")
                run_id = runs[0].get("dag_run_id")
                
            summary.append({
                "dag_id": dag_id,
                "status": status,
                "execution_date": execution_date,
                "run_id": run_id
            })
            
        return summary
