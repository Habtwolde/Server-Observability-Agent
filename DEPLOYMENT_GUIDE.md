# SQL Server Observability Agent — Client Deployment Guide

> **Purpose:** Production deployment and handoff procedure for the SQL Server Observability Agent in a client Databricks workspace.
>
> **Target operating model:** 45 monitored SQL Servers, daily SQL diagnostic workbooks, Windows Events evidence, Databricks-native alert routing, subscriber-specific email notifications, and a Streamlit Databricks App.

---

## 1. Production architecture at a glance

The production flow is:

```text
Daily source refresh
    │
    ├── 45 SQL diagnostic workbooks
    └── 1 Windows Events CSV
            │
            ▼
01_Register_and_Validate_Daily_Run
            │
            ▼
02_Ingest_SQL_Diagnostics_Workbooks
            │
            ▼
03_Ingest_Windows_Events
            │
            ▼
04_Build_Analysis_Ready_Tables
            │
            ▼
05_Evaluate_Server_Health_Rules
            │
            ▼
06_Prepare_Databricks_Alert
            │
            ▼
Subscriber-specific Databricks SQL Alerts
            │
            ▼
Databricks-native email notifications
```

The Streamlit application reads the same curated Agent tables/views and provides:

- **Fleet priorities**
- **Server diagnosis**
- **Pipeline status**
- **Notification subscriptions**
- Grounded Agent assistance through Databricks model serving

The notification model is **many-to-many**:

- one subscriber can receive alerts for one or many servers;
- one server can have one or many subscribers;
- a subscriber receives only findings for the servers assigned to that subscriber.

---

# PART A — BEFORE DEPLOYMENT

## 2. Required client-side prerequisites

Before importing the project, confirm that the client has the following Databricks capabilities.

### Workspace / platform

- Unity Catalog enabled.
- Permission to create or use a catalog and schema.
- A Unity Catalog volume available for the daily source files.
- Serverless notebook/job compute available, or an approved alternative compute configuration.
- A Databricks SQL Warehouse.
- Databricks Apps enabled.
- A model serving endpoint that the application can query.
- A Databricks **workspace admin** available for the notification-destination provisioning step.

### Recommended production identities

Use two identities where possible:

1. **Deployment administrator**
   - imports the project;
   - creates/configures the App;
   - runs the initial setup notebooks;
   - provisions notification destinations and SQL Alerts.

2. **Production Workflow service principal**
   - recommended as the final `Run as` identity for the daily Workflow;
   - should not depend on an individual employee account remaining active.

---

## 3. Environment values to confirm before running anything

The repository currently assumes these defaults:

| Setting | Current default | Client action |
|---|---|---|
| Catalog | `ent_log_analytics` | Keep or replace consistently |
| Schema | `observability` | Keep or replace consistently |
| Volume | `server_observability_vol` | Must exist before Notebook 00 runs |
| Expected SQL Servers | `45` | Confirm against approved production scope |
| Business timezone | `America/New_York` | Confirm with operations |
| Workflow time | `11:15 AM` | Confirm it remains after the source refresh |
| SQL Alert minimum severity | `HIGH` | Current production behavior includes HIGH + CRITICAL |
| Priority issues/server | `5` | May be set to 3 or 5 |
| Workflow name | `prod-server-observability-agent-daily` | Normally keep |
| Alert prefix | `prod-server-observability-agent-priority` | Normally keep |

### Files containing environment-specific values

Before client deployment, review:

```text
notebooks/00_Setup_Agent_Storage_and_Registry.py
notebooks/06_Prepare_Databricks_Alert.py
notebooks/07_Create_Daily_Automation_Job.py
app.yaml
```

In particular, `app.yaml` must not retain the developer workspace values for:

```text
DATABRICKS_HOST
DATABRICKS_WAREHOUSE_ID
MODEL_ENDPOINT_NAME
MODEL_ENDPOINT_FALLBACK
```

Set them to resources that exist in the client workspace.

---

# PART B — DATA PLATFORM SETUP

## 4. Import the repository into the client workspace

Clone or import the final repository into the client Databricks workspace.

Keep the repository structure intact:

```text
Server-Observability-Agent/
├── app.py
├── app.yaml
├── assets/
├── db/
├── notebooks/
│   ├── 00_Setup_Agent_Storage_and_Registry.py
│   ├── 01_Register_and_Validate_Daily_Run.py
│   ├── 02_Ingest_SQL_Diagnostics_Workbooks.py
│   ├── 03_Ingest_Windows_Events.py
│   ├── 04_Build_Analysis_Ready_Tables.py
│   ├── 05_Evaluate_Server_Health_Rules.py
│   ├── 06_Prepare_Databricks_Alert.py
│   └── 07_Create_Daily_Automation_Job.py
├── services/
├── tests/
├── ui/
└── requirements.txt
```

**Do not move individual notebooks into unrelated workspace folders.** Notebook 07 derives the task notebook folder from its own deployed location.

---

## 5. Prepare the Unity Catalog volume

Notebook 00 expects the Unity Catalog volume itself to already exist.

Default volume root:

```text
/Volumes/ent_log_analytics/observability/server_observability_vol
```

If the client uses different names, update the catalog/schema/volume constants consistently before running the setup.

Notebook 00 will create the required Agent directories inside the volume, including:

```text
raw/sql_diagnostics/inbox
raw/sql_diagnostics/by_server
raw/windows_events/inbox
agent/quarantine/sql_diagnostics
agent/quarantine/windows_events
agent/run_manifests
agent/checkpoints/sql_diagnostics
agent/checkpoints/windows_events
agent/audit_logs
```

---

## 6. Run Notebook 00 — one-time Agent storage and registry setup

Run:

```text
00_Setup_Agent_Storage_and_Registry
```

This notebook creates/validates the Agent configuration and storage contract, including:

- `agent_config`
- `agent_server_registry`
- `agent_alert_subscriptions`
- `agent_alert_payload`
- `agent_ingestion_runs`
- `agent_source_files`
- `agent_sheet_manifest`
- `agent_sql_diagnostics_bronze`
- `agent_windows_events_bronze`

Expected completion message:

```text
SQL Server Observability Agent setup validation succeeded.
```

Do not continue if Notebook 00 cannot access the Unity Catalog volume or reports missing tables.

---

# PART C — FIRST PRODUCTION DATA SET AND SERVER REGISTRY

## 7. Stage the first complete production source set

The initial production bootstrap requires a complete source set for one business date.

### SQL diagnostics

Place **exactly one current workbook per monitored server** into:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/sql_diagnostics/inbox
```

Supported workbook types:

```text
.xlsx
.xlsm
```

For the standard collector format, the first worksheet must identify the SQL Server using:

```text
A1 = Server Name
A2 = <server identifier>
```

The workbook-reported identity is authoritative.

### Windows Events

Place the current Windows Events CSV into:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/windows_events/inbox
```

Supported type:

```text
.csv
```

### Important date rule

Notebook 01 classifies source files using the configured business timezone. For the production run date, the source files must be recognized as current for that date; stale files are ignored.

---

## 8. Bootstrap the authoritative 45-server registry

The scheduled Workflow deliberately uses:

```text
bootstrap_registry = false
```

Therefore the registry must be initialized **before** normal scheduling begins.

Run Notebook 01 manually for the first complete source set:

```text
01_Register_and_Validate_Daily_Run
```

Use:

```text
Execution mode        = PRODUCTION
Bootstrap registry    = true
Fail when incomplete  = true
Run trigger           = MANUAL_BOOTSTRAP
Run date              = blank for current business date
                        or the exact intended YYYY-MM-DD date
```

Bootstrap succeeds only when the inbox contains the expected complete set — by default:

```text
45 valid, unique SQL diagnostic workbooks
+ 1 Windows Events CSV
```

Do not bypass this control in production.

### Acceptance checkpoint

Confirm:

- the registry contains the approved production servers;
- there are no duplicate server identities;
- there are no unexpected servers;
- there are no invalid workbooks;
- the run reaches the production-ready validation state.

---

# PART D — INITIAL ANALYTICS VALIDATION

## 9. Validate the analytical notebooks once before enabling notifications

Using the run ID created by Notebook 01, run the processing chain once in order:

```text
02_Ingest_SQL_Diagnostics_Workbooks
03_Ingest_Windows_Events
04_Build_Analysis_Ready_Tables
05_Evaluate_Server_Health_Rules
```

For production validation:

```text
allow_incomplete_run = false
```

Expected outcome:

- SQL diagnostic evidence is ingested;
- Windows Events evidence is ingested;
- analysis-ready tables/views are built;
- deterministic health rules are evaluated;
- the run reaches:

```text
HEALTH_RULES_EVALUATED
```

Do not proceed to production notification setup if the run is incomplete or test-only.

---

# PART E — DATABRICKS APP DEPLOYMENT

## 10. Configure the client-specific `app.yaml`

Before deploying the App, replace developer-specific values with client values.

At minimum verify:

```text
DATABRICKS_HOST
DATABRICKS_WAREHOUSE_ID
OBS_CATALOG
OBS_SCHEMA
MODEL_ENDPOINT_NAME
MODEL_ENDPOINT_FALLBACK
```

The model endpoint names must exist and be queryable in the client workspace.

---

## 11. Create the Databricks App

Create a Databricks App from the repository source and configure App resources before final deployment.

### Required App resource — SQL Warehouse

Add the selected SQL Warehouse with:

```text
Permission: Can use
```

### Required App resource — model serving

Add the primary model serving endpoint with:

```text
Permission: Can query
```

If the configured fallback endpoint is retained, also grant the App permission to query that endpoint.

### Required App data access

The App reads Agent data from the configured observability schema.

Grant **Select** access, directly or through App resources, for the read-only relations used by the UI, including the configured equivalents of:

```text
agent_ingestion_runs
agent_server_registry
agent_source_files
agent_sheet_manifest
agent_sql_rows_silver
agent_sql_metric_facts
agent_windows_events_silver
v_agent_latest_server_daily_inventory
agent_rule_catalog
agent_findings
v_agent_current_findings
v_agent_top_server_findings
v_agent_latest_server_health_summary
```

### Required writable App resource

The subscription UI writes to:

```text
agent_alert_subscriptions
```

Add this UC table to the App with:

```text
Permission: Modify
```

`Modify` is required because the App adds, reactivates, and deactivates subscriber-to-server routing records.

### App access for users

Grant normal users:

```text
CAN USE
```

Reserve App management permissions for administrators/maintainers.

---

## 12. Deploy and smoke-test the App

Deploy/start the App.

Confirm all four operational tabs load:

```text
Fleet priorities
Server diagnosis
Pipeline status
Notification subscriptions
```

Also confirm:

- the Server selector shows the production registry;
- the latest production pipeline run is visible;
- fleet/server findings are visible;
- the Agent assistant can query the configured model serving endpoint.

Do not configure the daily Workflow until the App can read the production Agent data successfully.

---

# PART F — SUBSCRIBERS AND DATABRICKS-NATIVE EMAIL ALERTS

## 13. Add a pilot subscriber through the App

For the first production email test, start with one designated client administrator/DBA and one representative server.

In:

```text
Notification subscriptions
```

enter:

```text
Subscriber email
Server(s)
Optional note
```

Then select:

```text
Add subscription
```

The routing design supports:

```text
1 email  → many servers
1 server → many emails
```

The App does **not** hardcode recipients.

---

## 14. Run Notebook 07 — create notification destinations, SQL Alerts, and Workflow

Run:

```text
07_Create_Daily_Automation_Job
```

The deployment user should be a **workspace admin** for this step because the notebook provisions/resolves Databricks notification destinations.

Use the client's SQL Warehouse ID.

For production deployment use:

```text
Workflow execution mode = PRODUCTION
TEST run date            = blank
Schedule state           = PAUSED
Priority issues/server   = 5   # or approved value 3
```

Keep the schedule **PAUSED** during commissioning.

Notebook 07 will create/update:

- one email notification destination per distinct active subscriber;
- one subscriber-specific Databricks SQL Alert per distinct active subscriber;
- the daily Workflow;
- one alert task per active subscriber.

The expected Workflow chain is:

```text
validate_daily_run
    ↓
ingest_sql_diagnostics
    ↓
ingest_windows_events
    ↓
build_analysis_tables
    ↓
evaluate_health_rules
    ↓
prepare_databricks_alert
    ↓
notify_subscriber_001
notify_subscriber_002
...
```

### Critical safety settings

For production, verify:

```text
Execution mode       = PRODUCTION
allow_incomplete_run = false
Workflow run date    = CURRENT BUSINESS DATE
Schedule state       = PAUSED
```

Do **not** enable the Notebook 06 option:

```text
Allow TEST run email delivery = true
```

That switch exists only for controlled commissioning/testing and defaults to `false`.

---

## 15. Set the Workflow production identity

Recommended after Notebook 07 creates the Workflow:

```text
Jobs & Pipelines
→ prod-server-observability-agent-daily
→ Edit
→ Run as
→ dedicated production service principal
```

The production identity must have the permissions required by Notebooks 01–06, including access to:

- the repository notebooks;
- the Unity Catalog catalog/schema;
- Agent tables/views;
- the source Unity Catalog volume;
- the selected SQL Warehouse where applicable.

If the Workflow remains configured to run as the deployment user, that user account must remain active and adequately privileged.

---

# PART G — COMMISSIONING TEST

## 16. Run the complete Workflow manually while the schedule remains paused

With a complete, current production source set available:

```text
Jobs & Pipelines
→ prod-server-observability-agent-daily
→ Run now
```

Do **not** resume the schedule yet.

Monitor every task.

Expected result:

```text
validate_daily_run        → Succeeded
ingest_sql_diagnostics    → Succeeded
ingest_windows_events     → Succeeded
build_analysis_tables     → Succeeded
evaluate_health_rules     → Succeeded
prepare_databricks_alert  → Succeeded
notify_subscriber_*       → Succeeded
```

### Email acceptance test

For the pilot subscriber, confirm the Databricks-native email contains only that subscriber's assigned server(s) and presents:

- server name;
- snapshot date;
- health status;
- critical/high/priority issue counts;
- ranked priority findings;
- evidence;
- likely cause;
- immediate action;
- Microsoft reference links.

A subscriber must never receive findings for a server that is not assigned to that subscriber.

---

# PART H — FULL SUBSCRIBER ROLLOUT

## 17. Configure the real recipient map

After the pilot succeeds, use the App to add the actual client recipient/server assignments.

Examples:

```text
DBA A → Server01, Server02, Server03
DBA B → Server03
DBA C → Server04, Server05
```

After adding or removing subscriptions, rerun:

```text
07_Create_Daily_Automation_Job
```

This synchronizes the active subscriber routing into Databricks notification destinations, SQL Alerts, and Workflow alert tasks.

Keep the Workflow paused while changing the production recipient map.

---

# PART I — GO LIVE

## 18. Final pre-go-live checklist

Before unpausing the Workflow, verify all of the following.

### Data

- [ ] All 45 approved servers are active in `agent_server_registry`.
- [ ] The daily collector delivers one current SQL diagnostic workbook per expected server.
- [ ] The current Windows Events CSV is delivered.
- [ ] No duplicate server workbooks exist.
- [ ] No stale/test source files are being treated as current production evidence.

### Analytics

- [ ] Production run reaches `HEALTH_RULES_EVALUATED`.
- [ ] `allow_incomplete_run = false` in scheduled tasks.
- [ ] Notebook 06 TEST email delivery is disabled.

### App

- [ ] App opens successfully for intended users.
- [ ] SQL Warehouse resource has `Can use`.
- [ ] Model endpoint resource has `Can query`.
- [ ] Read-only Agent relations are queryable.
- [ ] `agent_alert_subscriptions` has `Modify` for the App.

### Notifications

- [ ] Only real client recipient addresses are active.
- [ ] Each subscriber is mapped only to approved server(s).
- [ ] Subscriber-specific SQL Alerts are present.
- [ ] Pilot production email was received and reviewed.

### Workflow

- [ ] Workflow is `prod-server-observability-agent-daily`.
- [ ] Schedule is `11:15 AM America/New_York` unless client operations approved another time.
- [ ] Run identity is a durable production identity.
- [ ] One complete manual production Workflow run succeeded.

---

## 19. Enable the daily schedule

Only after the entire checklist passes:

```text
Jobs & Pipelines
→ prod-server-observability-agent-daily
→ Resume
```

Default production schedule:

```text
11:15 AM America/New_York
```

This schedule is intended to run after the client's source refresh.

If the client changes the upstream refresh time, move the Agent schedule far enough after the refresh to ensure the complete daily source set is present before validation begins.

---

# PART J — DAILY OPERATIONS

## 20. Normal daily behavior

Each scheduled run should:

1. validate the complete current source set;
2. reject incomplete production input;
3. ingest SQL diagnostic evidence;
4. ingest Windows Events evidence;
5. rebuild analysis-ready data;
6. evaluate deterministic health rules;
7. select CRITICAL/HIGH priority findings;
8. prepare subscriber-routed alert payloads;
9. evaluate subscriber-specific SQL Alerts;
10. send Databricks-native email notifications where the alert condition is triggered.

---

## 21. If the Workflow fails

Start with the **first failed task**, not the downstream blocked tasks.

### `validate_daily_run` failure

Check:

- missing SQL workbook(s);
- stale modification dates;
- duplicate server identity;
- invalid workbook identity;
- missing Windows Events CSV;
- server registry mismatch.

### SQL ingestion failure

Check the failing workbook and the worksheet manifest.

### Windows Events ingestion failure

Check the current Windows CSV format and source-file record.

### Health-rule failure

Inspect the analysis-ready tables and the exact run ID passed from upstream tasks.

### Alert preparation failure

Check:

- run status;
- `agent_alert_payload`;
- `v_agent_databricks_alert_payload`;
- `v_agent_databricks_alert_routes`;
- active subscription routing.

### Email/Alert failure

Check:

- subscriber email spelling;
- `notification_destination_id` in `agent_alert_subscriptions`;
- subscriber-specific SQL Alert history;
- failed notification destinations in the Alert UI.

---

# PART K — CHANGE MANAGEMENT

## 22. Add a subscriber

1. Open the App.
2. Go to `Notification subscriptions`.
3. Add the email and assigned server(s).
4. Pause the Workflow if it is currently active.
5. Rerun Notebook 07 in `PRODUCTION` mode.
6. Verify the new subscriber alert task.
7. Resume the Workflow.

---

## 23. Remove a subscriber or server assignment

1. Open `Notification subscriptions`.
2. Select the subscriber.
3. Select the server assignment(s) to remove.
4. Remove the selected subscription(s).
5. Pause the Workflow if needed.
6. Rerun Notebook 07 so the active Workflow task set reflects the current routing map.
7. Verify the active subscription list.
8. Resume the Workflow.

Subscription rows are deactivated for auditability rather than silently overwritten.

---

## 24. Add or remove a monitored SQL Server

Changing the monitored server population is a controlled production change.

Update and validate:

- `agent_server_registry`;
- the expected server count if the approved scope is no longer 45;
- upstream collector delivery;
- subscriber assignments;
- validation logic and commissioning evidence.

Do not simply lower the expected server count to bypass missing production data.

---

## 25. Change the SQL Warehouse

If the client moves the Agent to another SQL Warehouse:

1. update the Warehouse resource on the App;
2. update `DATABRICKS_WAREHOUSE_ID` in the deployed App configuration;
3. use the same Warehouse ID when rerunning Notebook 07;
4. verify the subscriber SQL Alerts reference the intended Warehouse;
5. perform a manual commissioning run before resuming scheduling.

---

## 26. Change the model serving endpoint

If the model endpoint changes:

1. update `MODEL_ENDPOINT_NAME` and, if used, `MODEL_ENDPOINT_FALLBACK`;
2. add the new serving endpoint as an App resource;
3. grant `Can query`;
4. redeploy the App;
5. test the Agent assistant.

The deterministic health findings and email alert selection remain grounded in the rule-engine outputs; the model endpoint is used for the interactive AI layer and related AI-assisted services.

---

# PART L — EMERGENCY CONTROLS

## 27. Immediate stop

To stop automated processing and notifications without deleting any configuration:

```text
Jobs & Pipelines
→ prod-server-observability-agent-daily
→ Pause
```

The subscriber SQL Alerts created by Notebook 07 have their independent schedules kept paused; the Workflow controls their evaluation.

---

## 28. Production safety rules

Never use these shortcuts to force a production run through incomplete data:

```text
execution_mode = TEST
allow_incomplete_run = true
Allow TEST run email delivery = true
```

Those options exist only for controlled development/commissioning scenarios.

The production state should be:

```text
execution_mode          = PRODUCTION
allow_incomplete_run    = false
allow_test_email_delivery = false
```

---

# PART M — CLIENT ACCEPTANCE RECORD

## 29. Recommended sign-off evidence

Retain evidence of the following before formal handoff:

- successful Notebook 00 setup output;
- approved 45-server registry extract;
- successful first production validation run;
- successful full 7-stage Workflow run;
- App screenshots for the four operational tabs;
- pilot subscriber mapping;
- successful Databricks SQL Alert history;
- received production-format email sample;
- final active subscriber/server mapping;
- final Workflow schedule and `Run as` identity.

---

## 30. Final acceptance checklist

The deployment can be considered operational when:

```text
[ ] Unity Catalog setup is complete
[ ] Production volume is accessible
[ ] 45-server registry is approved
[ ] Complete daily input is arriving
[ ] Analytics chain succeeds
[ ] Streamlit App succeeds
[ ] App resources/permissions succeed
[ ] Client subscriptions are configured
[ ] Databricks notification destinations succeed
[ ] Subscriber-specific SQL Alerts succeed
[ ] Pilot email is received correctly
[ ] Full Workflow succeeds manually
[ ] Workflow Run-as identity is production-safe
[ ] Daily schedule is enabled only after acceptance
```

---

# Official Databricks references

- Databricks Apps resources: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/resources
- SQL Warehouse App resource: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/sql-warehouse
- Model serving App resource: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/model-serving
- Unity Catalog table App resource: https://docs.databricks.com/gcp/en/dev-tools/databricks-apps/tables
- Databricks App permissions: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/permissions
- Notification destinations: https://docs.databricks.com/aws/en/admin/workspace-settings/notification-destinations

---

**Document status:** Client deployment / production handoff guide  
**Notification method:** Databricks-native email destinations and SQL Alerts  
**Production data gate:** Complete authoritative server registry and complete daily source set
