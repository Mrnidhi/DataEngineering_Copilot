# DataSight AI 🔬

**Self-healing observability plugin for Apache Airflow.**

Autonomous failure detection → LLM-powered root-cause analysis → engineer-approved auto-remediation.

---

## ✨ Features

| Feature | Description |
|---|---|
| **🔌 One-Line Install** | `pip install datasight-ai` — works with any Airflow 2.7+ deployment |
| **⚡ Real-Time Detection** | Hooks into Airflow's Listener API — zero polling, instant failure capture |
| **🔍 Intelligent Diagnosis** | Reads task logs + DAG source code + referenced SQL/dbt models → LLM root-cause analysis |
| **🤖 Auto-Fix Generation** | Generates minimal code patches with unified diffs |
| **✅ Approval Workflow** | Multi-channel approvals: Airflow UI, Streamlit dashboard, Slack, GitHub PRs |
| **🔧 Self-Healing** | Once approved, patches are auto-applied and DAGs are re-triggered |
| **🔒 Backup & Rollback** | Every patch creates a backup — one-click rollback if needed |
| **🧠 Configurable LLM** | Ollama (local, free) or OpenAI/Anthropic (cloud) |
| **📦 Git Integration** | Auto-creates branches and PRs for production environments |

---

## 🚀 Quick Start

### 1. Install
```bash
pip install datasight-ai[all]
```

### 2. Configure (add to your `docker-compose.yaml`)
```yaml
environment:
  - AIRFLOW__LISTENERS__LISTENER_CLASS=datasight.listener.listener.DataSightListener
  - DATASIGHT_LLM_PROVIDER=ollama
  - DATASIGHT_LLM_MODEL=llama3.2:8b
```

### 3. That's it! 🎉
DataSight automatically:
- Registers its Listener for real-time failure detection
- Adds a "DataSight" menu in the Airflow Web UI
- Begins monitoring all DAG tasks

---

## 📦 Architecture

```
datasight/
├── config/              # Pydantic Settings (env vars)
├── listener/            # Airflow Listener API hook
├── analyzer/
│   ├── log_analyzer     # Log fetching & traceback extraction
│   └── code_analyzer    # DAG source + SQL + dbt reader
├── llm/
│   ├── engine           # Unified LLM interface
│   └── providers/       # Ollama, OpenAI backends
├── remediation/
│   └── patcher          # Apply fixes + trigger reruns
├── approval/
│   ├── gateway          # Approval state machine
│   ├── models           # Incident & Patch data models
│   └── channels/        # Slack, GitHub PR notifications
├── git/                 # Git branch/commit/PR operations
├── airflow_plugin/      # Native Airflow Web UI views
├── api/                 # FastAPI REST endpoints
└── ui/                  # Streamlit monitoring dashboard
```

---

## ⚙️ Configuration

All settings via environment variables (prefix: `DATASIGHT_`):

| Variable | Default | Description |
|---|---|---|
| `DATASIGHT_ENABLED` | `true` | Master kill switch |
| `DATASIGHT_LLM_PROVIDER` | `ollama` | `ollama` or `openai` |
| `DATASIGHT_LLM_MODEL` | `llama3.2:8b` | Model name |
| `DATASIGHT_OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama endpoint |
| `DATASIGHT_OPENAI_API_KEY` | — | OpenAI API key |
| `DATASIGHT_GIT_ENABLED` | `false` | Enable Git integration |
| `DATASIGHT_GIT_REPO_URL` | — | Git repository URL |
| `DATASIGHT_APPROVAL_REQUIRED` | `true` | Require engineer approval |
| `DATASIGHT_APPROVAL_CHANNELS` | `ui` | `ui`, `slack`, `github_pr` |
| `DATASIGHT_SLACK_WEBHOOK_URL` | — | Slack webhook for notifications |

---

## 🧪 Development

```bash
git clone https://github.com/Mrnidhi/DataEngineering_Copilot.git
cd DataEngineering_Copilot
pip install -e ".[dev,all]"
```

---

## 📄 License

Apache 2.0
