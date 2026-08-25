# SQL Server Observability Agent — production handoff

## What is included

- `06_Send_Priority_Email.py`: sends one consolidated priority-only email to
  `habtwolde5@gmail.com`; includes the top 5 CRITICAL/HIGH issues per affected
  server, their evidence, likely cause, immediate action, and Microsoft link.
- `07_Create_Daily_Automation_Job.py`: creates or updates the six-task daily
  Databricks Workflow.
- `app_update/`: updates the Streamlit Agent with the fixed sticky title and the
  latest email-delivery status.

The Workflow runs daily at **11:15 AM America/New_York**, after the client's
11:00 AM file refresh. It is created **PAUSED** to prevent accidental production
execution while only sample inputs are present.

## Required setup

1. Upload notebooks `06_Send_Priority_Email.py` and
   `07_Create_Daily_Automation_Job.py` into the existing Agent `notebooks`
   folder. Databricks will display them without the `.py` extension.

2. Create the Databricks secret scope:

   `server-observability-agent`

   Workspace UI address:

   `https://adb-4480026081424261.1.azuredatabricks.net#secrets/createScope`

3. Add these two secrets. Do not paste their values into a notebook:

   - `smtp-user`: the Gmail address used as the sender.
   - `smtp-app-password`: a Google App Password, not the normal Gmail password.

   Safe interactive CLI commands:

   ```bash
   databricks secrets put-secret server-observability-agent smtp-user
   databricks secrets put-secret server-observability-agent smtp-app-password
   ```

   Google App Passwords require 2-Step Verification on the sending account.

4. Run `07_Create_Daily_Automation_Job` once with its defaults. It validates
   notebook paths and creates/updates:

   `prod-server-observability-agent-daily`

5. For one email test only, run `06_Send_Priority_Email` manually with:

   - `allow_test_run = true`
   - `recipient_email = habtwolde5@gmail.com`
   - `force_resend = false`

   The sample-run email is clearly marked `[TEST]`.

6. Keep the daily Workflow paused until the registry contains the 45 approved
   servers and the inbox has all 45 current diagnostic workbooks plus the
   current Windows Events CSV. Then unpause the Workflow.

## Production safeguards

- The scheduled Workflow uses `fail_on_incomplete=true`.
- Test and historical runs cannot send through the scheduled job.
- Only CRITICAL/HIGH findings are emailed.
- Healthy and informational observations are omitted.
- Rerunning the same run does not resend an already sent email unless
  `force_resend=true` is explicitly selected.
- Delivery attempts and failures are audited in
  `ent_log_analytics.observability.agent_notification_log`.
- Databricks also sends a native job-failure notification to
  `habtwolde5@gmail.com`.

