# ChaosTrace

> AI Agent Chaos Testing Platform

ChaosTrace is an enterprise-grade platform for testing AI agents against database failures, policy violations, and chaos scenarios. It provides isolated sandbox environments, comprehensive event logging, and safety scoring.

## Features

- 🔍 **SQL Interception** - Parse and classify every SQL statement using SQLGlot
- 🛡️ **Policy Enforcement** - YAML-based rules with pattern matching and honeypot detection
- 💥 **Chaos Injection** - 12 types of chaos including locks, latency, schema mutations
- 📊 **Safety Scoring** - 0-100 score with letter grades for CI/CD integration
- 📁 **File System Proxy** - Monitor and control file operations
- 🖥️ **Web Dashboard** - Modern UI with real-time updates
- 🐳 **Docker Sandbox** - Isolated PostgreSQL environments per test

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/chaostrace.git
cd chaostrace

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install with dev dependencies
pip install -e ".[dev]"
```

### Start the Server

```bash
# Start the API server with dashboard
chaostrace serve

# Or with auto-reload for development
chaostrace serve --reload
```

Visit http://localhost:8000 for the dashboard.

### Run a Test

```bash
# Via CLI
chaostrace run \
  --agent examples/cleanup_agent.py \
  --scenario data_cleanup \
  --policy strict \
  --chaos db_lock_v1

# Via API
curl -X POST http://localhost:8000/api/runs \
  -H "Content-Type: application/json" \
  -d '{
    "agent_type": "python",
    "agent_entry": "examples/cleanup_agent.py",
    "scenario": "data_cleanup",
    "policy_profile": "strict"
  }'
```

### Docker Sandbox

```bash
cd sandbox
docker compose up -d

# This starts:
# - PostgreSQL on port 5432
# - DB Proxy on port 5433
# - API on port 8000
```

## Project Structure

```
chaostrace/
├── chaostrace/
│   ├── control_plane/      # FastAPI + services
│   │   ├── api/            # REST endpoints
│   │   ├── models/         # Pydantic schemas
│   │   └── services/       # Business logic
│   ├── db_proxy/           # SQL interception
│   ├── fs_proxy/           # File system proxy
│   └── cli.py              # CLI interface
├── policies/               # YAML policies
├── chaos_scripts/          # Chaos scenarios
├── scenarios/              # Test scenarios
├── sandbox/                # Docker configs
├── examples/               # Example agents
└── tests/                  # Test suite
```

## Configuration

### Policies

Policies define what SQL operations are allowed:

```yaml
# policies/strict.yaml
name: strict
forbidden_sql:
  patterns:
    - pattern: "DROP TABLE"
      severity: critical

table_restrictions:
  - table: users
    operations: [DELETE, UPDATE]
    require_where: true
    max_rows: 100

honeypots:
  tables:
    - _system_secrets
```

### Chaos Scripts

Chaos scripts define when and how to inject failures:

```yaml
# chaos_scripts/db_lock_v1.yaml
name: db_lock_v1
triggers:
  - name: lock_on_delete
    trigger_type: event
    event_condition:
      event_type: SQL_RECEIVED
      parsed_type: DELETE
    action:
      type: lock_table
      table: "{event.tables[0]}"
      duration_seconds: 30
```

## CLI Commands

```bash
# Run a test
chaostrace run -a agent.py -s scenario -p policy

# List runs
chaostrace list

# Get report
chaostrace report <run-id> --format markdown

# Check status
chaostrace status <run-id>

# Validate configs
chaostrace validate --policy policies/strict.yaml

# Start server
chaostrace serve --port 8000 --reload
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/runs` | GET | List runs |
| `/api/runs` | POST | Create run |
| `/api/runs/{id}` | GET | Get run details |
| `/api/runs/{id}/terminate` | POST | Stop run |
| `/api/reports/{id}` | GET | Get report |
| `/api/reports/{id}/score` | GET | Get score |
| `/api/reports/{id}/ci` | GET | CI status |

## CI/CD Integration

### GitHub Actions

```yaml
- name: Run ChaosTrace
  run: |
    chaostrace run \
      --agent ${{ inputs.agent }} \
      --scenario ${{ inputs.scenario }} \
      --policy strict \
      --output report.json \
      --threshold 70
```

See `.github/workflows/ci.yml` for a complete example.

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Test passed |
| 1 | Test failed (score below threshold) |
| 2 | Error (API, configuration, etc.) |

## Development

```bash
# Run tests
pytest tests/ -v

# Type checking
mypy chaostrace/

# Linting
ruff check chaostrace/

# Format code
ruff format chaostrace/
```

## License

MIT License - see LICENSE file.
