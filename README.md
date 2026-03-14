# DataSight AI: Autonomous Airflow Observability

DataSight AI transforms passive monitoring into active intelligence. By connecting a Local Large Language Model (Llama 3.2 via Ollama) and a Retrieval-Augmented Generation (RAG) knowledge base to Apache Airflow, this tool actively monitors your data pipelines, detects failures in real-time, and automatically diagnoses the root cause.

## 🚀 Features

- **Real-Time DAG Monitoring:** A clean, dark-mode Streamlit Command Center that displays the live status of all your Apache Airflow pipelines.
- **Autonomous Anomaly Detection:** Automatically spots when an Airflow task fails.
- **AI Diagnostics:** Instead of manually digging through logs, the AI fetches the failed task's traceback and cross-references it against official documentation (Apache Spark, dbt, Airflow) to tell you *exactly* what went wrong and how to fix it.
- **100% Local & Secure:** Uses local vector embeddings (ChromaDB) and local LLM execution (Ollama) so no sensitive pipeline logs ever leave your machine.
- **Ad-Hoc Copilot:** A built-in chat interface to ask architectural questions strictly grounded in the indexed documentation.

## 🏗️ Architecture

1. **Airflow Connection:** Uses the Airflow REST API to continuously poll DAG and Task Instance states.
2. **RAG Knowledge Base:** Web scrapers load official Airflow, dbt, and Spark documentation into a local ChromaDB vector space.
3. **AI Engine:** When a failure is detected, the raw traceback is fed to `Llama 3.2` along with the relevant retrieved documentation using a `ReAct` Agent framework to generate actionable fixes.
4. **Command Center UI:** A high-performance Streamlit dashboard visualizes the cluster health and expands active alerts.

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.11
- Docker & Docker Compose (for the Airflow cluster)
- Ollama (installed locally with the `llama3.2:3b-instruct-q4_K_S` model pulled)

### 1. Initialize the AI Environment
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start the Apache Airflow Cluster
DataSight needs an Airflow cluster to monitor. You can spin up a local dockerized cluster using the included `docker-compose.yaml`.

```bash
docker-compose up airflow-init
docker-compose up -d
```
*Wait ~2 minutes for the Airflow Webserver to become healthy.*

You can log into Airflow at `http://localhost:8080` (Username: `airflow`, Password: `airflow`). Two sample DAGs are included in the `/dags` folder to test the monitoring. Unpause them in the Airflow UI to simulate runs.

### 3. Launch DataSight AI
Start the AI Monitoring Dashboard:

```bash
PYTHONPATH=. streamlit run Rag_agent/services/streamlit.py
```

The Command Center will open at `http://localhost:8501`. 

## 🚨 Triggering the Copilot

To see the AI in action:
1. Ensure the `failing_dbt_pipeline` DAG is unpaused in your Airflow UI.
2. Wait for it to inevitably simulate a failure.
3. Open the DataSight AI dashboard.
4. A red Alert Panel will automatically populate on the right side.
5. Expand the "View AI Resolution Guide" to read the Local LLM's diagnosis of the dbt anomaly.
