# SQL Server Observability Agent — Client Deployment Guide

> Production deployment guide for the SQL Server Observability Agent in Databricks.

---

## 1. What the client does after deployment

The normal client process is intentionally simple:

1. Place the current SQL Server diagnostic workbooks in the configured SQL diagnostics inbox.
2. Place the current Windows Events CSV in the configured Windows Events inbox.
3. Add or remove email subscribers in the Streamlit app under **Notification subscriptions** whenever needed.
4. Run the Databricks job:

   `prod-server-observability-agent-daily`

5. The Agent processes the valid server data, evaluates health findings, and automatically sends each subscriber the priority findings for the server(s) assigned to that subscriber.

The client does **not** rerun the Workflow-creation notebook when subscribers change.

The client does **not** manually schedule individual SQL Alerts.

---

# PART A — ONE-TIME DEPLOYMENT

## 2. Import the repository

Import or clone the final repository into the client Databricks workspace.

The production notebook chain is:

```text
00_Setup_Agent_Storage_and_Registry
01_Register_and_Validate_Daily_Run
01A_Validate_Available_Daily_Run
02_Ingest_SQL_Diagnostics_Workbooks
03_Ingest_Windows_Events
04_Build_Analysis_Ready_Tables
05_Evaluate_Server_Health_Rules
06_Prepare_Databricks_Alert
07_Dispatch_Subscriber_Notifications
08_Create_Daily_Automation_Job
```

`08_Create_Daily_Automation_Job` is a deployment notebook. It is not part of the daily analytical execution chain.

---

## 3. Update client environment values

Before deployment, the client updates `app.yaml` for the client workspace.

At minimum verify:

- `DATABRICKS_HOST`
- `DATABRICKS_WAREHOUSE_ID`
- `OBS_CATALOG`
- `OBS_SCHEMA`
- model-serving endpoint names

The notebooks currently use these production defaults unless intentionally changed:

```text
Catalog: ent_log_analytics
Schema: observability
Volume: server_observability_vol
Source timezone: America/New_York
Expected registered SQL Servers: 45
```

If the client uses different names, update the corresponding notebook configuration before commissioning.

---

## 4. Prepare Databricks prerequisites

The client must have:

- Unity Catalog enabled;
- the required catalog and schema;
- the Unity Catalog volume used by the Agent;
- a Databricks SQL Warehouse;
- Databricks Apps enabled;
- an approved model-serving endpoint for the Agent UI;
- permission to run Databricks Jobs and notebooks;
- permission to create/update Databricks SQL Alerts;
- permission to use the selected SQL Warehouse.

For fully automatic subscriber onboarding, the identity running the production job must also be allowed to create Databricks email notification destinations. This is required when a new subscriber email is encountered for the first time.

---

## 5. Run the storage/setup notebook

Run:

```text
00_Setup_Agent_Storage_and_Registry
```

This creates or validates the Agent tables and required paths, including:

- `agent_config`
- `agent_server_registry`
- `agent_alert_subscriptions`
- `agent_alert_payload`
- `agent_ingestion_runs`
- ingestion/audit tables
- Agent folders inside the configured volume

Do not continue until Notebook 00 completes successfully.

---

## 6. Bootstrap the 45-server registry once

The authoritative server registry is initialized only once.

For this first bootstrap only:

1. Place the complete approved set of **45 unique SQL diagnostic workbooks** in the SQL diagnostics inbox.
2. Place **one valid Windows Events CSV** in the Windows Events inbox.
3. Run:

   `01A_Validate_Available_Daily_Run`

4. Set:

```text
bootstrap_registry = true
execution_mode = PRODUCTION
```

The underlying validator only bootstraps the registry from a complete validated first set.

After the registry has been created, normal daily runs use:

```text
bootstrap_registry = false
```

The saved production Workflow already uses `false`.

---

## 7. Deploy the Streamlit Databricks App

Deploy the application using the client-specific `app.yaml`.

The App must be able to read the Agent tables/views and update the subscription table.

At minimum, grant the App service principal the required access to:

- the configured SQL Warehouse;
- Agent tables/views used by the UI;
- the configured volume where required;
- `agent_alert_subscriptions` with write/update capability.

Confirm the application opens successfully and the following areas work:

- Fleet priorities
- Server diagnosis
- Pipeline status
- Notification subscriptions

---

## 8. Add the real subscribers

In the Streamlit app, open:

**Notification subscriptions**

For each subscriber:

1. Enter the subscriber email.
2. Select one or more servers.
3. Click **Add subscription**.

The relationship is many-to-many:

- one subscriber may receive notifications for several servers;
- one server may have several subscribers.

No email address should be hardcoded in the notebooks.

---

## 9. Create the production Workflow once

Run:

```text
08_Create_Daily_Automation_Job
```

For initial commissioning use:

```text
schedule_state = PAUSED
```

This creates or updates:

```text
prod-server-observability-agent-daily
```

The saved Workflow has a fixed seven-task chain:

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
dispatch_subscriber_notifications
```

The last task reads the current subscription table dynamically every time the job runs.

Therefore, adding or removing a subscriber later does **not** require rerunning Notebook 08.

---

# PART B — HOW THE DAILY JOB WORKS

## 10. Daily input validation

When `prod-server-observability-agent-daily` starts, the Agent validates the current source files.

A production run can continue when:

- the authoritative registry already exists;
- exactly one valid Windows Events CSV is available for the run date; and
- at least one registered SQL Server has a valid workbook.

Missing, duplicate, unexpected, or invalid SQL workbooks are recorded and excluded from normal server processing.

This means one bad or missing server file does not prevent valid servers from being analyzed.

---

## 11. Server eligibility for email notification

A subscriber email is considered only for a subscribed server that has complete data for the run.

The notification dispatcher requires the server inventory status to be:

```text
COMPLETE
```

This means the same run has:

- a valid ingested SQL diagnostic workbook; and
- Windows Events evidence for that server.

Servers without complete source evidence are not included in the normal subscriber priority email.

Their data-quality condition remains visible in the Agent pipeline/diagnostic views.

---

## 12. Automatic subscriber handling

During the final task:

```text
07_Dispatch_Subscriber_Notifications
```

Databricks automatically:

1. reads the **current active subscribers**;
2. creates or reuses the subscriber's Databricks email notification destination;
3. creates or updates the subscriber-specific SQL Alert;
4. keeps the individual SQL Alert schedule **PAUSED**;
5. evaluates those alerts from the running Workflow;
6. sends the priority email to the correct subscriber.

If a subscriber is added today, the subscriber is automatically included the next time the daily job runs.

If a subscription is removed, it is automatically excluded from future dispatch runs.

No Workflow rebuild is required.

---

## 13. Important scheduling rule

Do **not** schedule the individual subscriber SQL Alerts.

Keep them:

```text
PAUSED
```

Only the main job should be run or scheduled:

```text
prod-server-observability-agent-daily
```

The Workflow evaluates the subscriber alerts only after that day's ingestion and health analysis has completed.

---

# PART C — CLIENT DAILY OPERATION

## 14. Manual daily operation

If the client wants to start the process manually after files are delivered:

1. Put the SQL diagnostic workbooks in the configured inbox.
2. Put the Windows Events CSV in the configured inbox.
3. Open Databricks **Jobs & Pipelines**.
4. Open:

   `prod-server-observability-agent-daily`

5. Click **Run now**.
6. Wait for all seven tasks to complete.

Nothing else is required.

---

## 15. Automatic daily operation

If the source files are reliably delivered before the configured run time, the client can enable the saved Workflow schedule.

Default definition:

```text
11:15 AM
America/New_York
Daily
```

The individual subscriber alerts must remain paused.

---

# PART D — SUBSCRIBER CHANGES

## 16. Add a subscriber later

Do only this:

1. Open the Streamlit app.
2. Open **Notification subscriptions**.
3. Enter the new email.
4. Select the server(s).
5. Save the subscription.

Done.

The next production job run handles the new subscriber automatically.

---

## 17. Remove a subscriber later

Do only this:

1. Open **Notification subscriptions**.
2. Select the subscriber.
3. Select the server subscription(s) to remove.
4. Click **Remove selected subscription(s)**.

The removed routing is no longer used in future dispatch runs.

---

# PART E — GO-LIVE CHECKLIST

## 18. Before unpausing the production schedule

Confirm all of the following:

- [ ] Client-specific `app.yaml` is configured.
- [ ] Catalog, schema, and volume are correct.
- [ ] Notebook 00 completed successfully.
- [ ] The authoritative 45-server registry is initialized.
- [ ] SQL Warehouse access is working.
- [ ] Streamlit App opens successfully.
- [ ] Real subscribers are configured in the App.
- [ ] App can update `agent_alert_subscriptions`.
- [ ] Production job identity can use the SQL Warehouse.
- [ ] Production job identity can create/update SQL Alerts.
- [ ] Production job identity can create email notification destinations for new subscribers.
- [ ] Notebook 06 has TEST email delivery disabled.
- [ ] `prod-server-observability-agent-daily` contains exactly the fixed seven-task chain.
- [ ] Individual subscriber SQL Alert schedules are PAUSED.
- [ ] One controlled production run has completed successfully.
- [ ] A real subscriber received the expected priority email.

Only after these checks should the client enable the daily schedule, if automatic scheduling is required.

---

# PART F — EXPECTED CLIENT EXPERIENCE

After commissioning, the client should not need to manage the internal notebooks during normal operation.

The normal operating model is simply:

```text
Put source files in Databricks
        ↓
Add/remove subscribers in the App when needed
        ↓
Run prod-server-observability-agent-daily
        ↓
Agent validates and analyzes valid servers
        ↓
Agent checks current subscriptions
        ↓
Databricks sends subscriber-specific priority emails
```

That is the intended production handoff model.
