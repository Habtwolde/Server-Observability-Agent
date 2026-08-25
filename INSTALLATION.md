# SQL Server Observability Agent application update

Copy the contents of this package into the existing
`Server-Observability-Agent` workspace folder, preserving the `db/`,
`services/`, and `ui/` subfolders.

Files replaced:

- `app.py`
- `app.yaml`
- `db/observability_sources.py`

Files added:

- `services/agent_findings_service.py`
- `services/agent_ai_service.py`
- `ui/agent_styles.py`
- `ui/fleet_priority_view.py`
- `ui/server_diagnosis_view.py`
- `ui/pipeline_status_view.py`
- `ui/agent_assistant_panel.py`

Operational files supplied separately:

- `06_Send_Priority_Email.py`
- `07_Create_Daily_Automation_Job.py`
- `PRODUCTION_NEXT_STEPS.md`

The app now reads `agent_notification_log` when available and shows the latest
priority-email delivery status on the Pipeline status tab.

Existing files required by this update:

- `db/connection.py`
- `services/llm_service.py`
- `requirements.txt`

The old dashboard UI modules may remain temporarily; the new `app.py` does not
import them. Remove them only after this Agent application starts successfully.
