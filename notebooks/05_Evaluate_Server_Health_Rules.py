# Databricks notebook source
# 05_Evaluate_Server_Health_Rules

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


# -----------------------------------------------------------------------------
# 1. Runtime parameters and run gate
# -----------------------------------------------------------------------------

dbutils.widgets.text("run_id", "", "Run ID (blank = latest run)")
dbutils.widgets.dropdown(
    "allow_incomplete_run",
    "false",
    ["false", "true"],
    "Allow incomplete run for development testing",
)
dbutils.widgets.text(
    "top_issues_per_server",
    "5",
    "Number of top issues retained in each server summary",
)

# Operational thresholds are deliberately widgets/configuration, not hidden
# constants. Microsoft guidance is stored separately in the rule catalog.
dbutils.widgets.text("full_backup_max_hours", "36", "Maximum full-backup age (hours)")
dbutils.widgets.text("log_backup_max_hours", "2", "Maximum log-backup age (hours)")
dbutils.widgets.text("checkdb_max_days", "7", "Maximum successful CHECKDB age (days)")
dbutils.widgets.text("disk_critical_free_percent", "10", "Critical disk free percent")
dbutils.widgets.text("disk_high_free_percent", "20", "High disk free percent")
dbutils.widgets.text("io_critical_latency_ms", "50", "Critical I/O latency (ms)")
dbutils.widgets.text("io_high_latency_ms", "20", "High I/O latency (ms)")
dbutils.widgets.text("cpu_critical_percent", "90", "Critical sustained CPU percent")
dbutils.widgets.text("cpu_high_percent", "80", "High sustained CPU percent")
dbutils.widgets.text("blocking_critical_seconds", "300", "Critical blocking duration")
dbutils.widgets.text("blocking_high_seconds", "30", "High blocking duration")
dbutils.widgets.text("source_stale_days", "2", "Maximum source-data age in production")

CATALOG = "ent_log_analytics"
SCHEMA = "observability"


def table_name(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def integer_widget(name: str, minimum: int, maximum: int) -> int:
    raw_value = dbutils.widgets.get(name).strip()
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Widget {name} must be an integer: {raw_value}") from exc
    if not minimum <= value <= maximum:
        raise ValueError(
            f"Widget {name} must be between {minimum} and {maximum}: {value}"
        )
    return value


ALLOW_INCOMPLETE_RUN = (
    dbutils.widgets.get("allow_incomplete_run").strip().lower() == "true"
)
TOP_ISSUES_PER_SERVER = integer_widget("top_issues_per_server", 1, 10)
FULL_BACKUP_MAX_HOURS = integer_widget("full_backup_max_hours", 1, 720)
LOG_BACKUP_MAX_HOURS = integer_widget("log_backup_max_hours", 1, 168)
CHECKDB_MAX_DAYS = integer_widget("checkdb_max_days", 1, 90)
DISK_CRITICAL_FREE_PERCENT = integer_widget("disk_critical_free_percent", 1, 50)
DISK_HIGH_FREE_PERCENT = integer_widget("disk_high_free_percent", 2, 75)
IO_CRITICAL_LATENCY_MS = integer_widget("io_critical_latency_ms", 5, 5000)
IO_HIGH_LATENCY_MS = integer_widget("io_high_latency_ms", 1, 1000)
CPU_CRITICAL_PERCENT = integer_widget("cpu_critical_percent", 50, 100)
CPU_HIGH_PERCENT = integer_widget("cpu_high_percent", 25, 99)
BLOCKING_CRITICAL_SECONDS = integer_widget("blocking_critical_seconds", 5, 86400)
BLOCKING_HIGH_SECONDS = integer_widget("blocking_high_seconds", 5, 3600)
SOURCE_STALE_DAYS = integer_widget("source_stale_days", 0, 30)

if DISK_CRITICAL_FREE_PERCENT >= DISK_HIGH_FREE_PERCENT:
    raise ValueError("Critical disk-free threshold must be below the High threshold.")
if IO_HIGH_LATENCY_MS >= IO_CRITICAL_LATENCY_MS:
    raise ValueError("High I/O threshold must be below the Critical threshold.")
if CPU_HIGH_PERCENT >= CPU_CRITICAL_PERCENT:
    raise ValueError("High CPU threshold must be below the Critical threshold.")
if BLOCKING_HIGH_SECONDS >= BLOCKING_CRITICAL_SECONDS:
    raise ValueError("High blocking threshold must be below the Critical threshold.")

run_id_parameter = dbutils.widgets.get("run_id").strip()

if run_id_parameter:
    RUN_ID = run_id_parameter
else:
    latest_runs = (
        spark.table(f"{CATALOG}.{SCHEMA}.agent_ingestion_runs")
        .orderBy(F.col("run_date").desc(), F.col("updated_ts").desc())
        .select("run_id")
        .limit(1)
        .collect()
    )
    if not latest_runs:
        raise RuntimeError("No Agent ingestion run exists. Run notebooks 01-04 first.")
    RUN_ID = latest_runs[0]["run_id"]

run_rows = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_ingestion_runs")
    .where(F.col("run_id") == RUN_ID)
    .orderBy(F.col("updated_ts").desc())
    .limit(1)
    .collect()
)

if not run_rows:
    raise RuntimeError(f"Run ID was not found: {RUN_ID}")

RUN_DATE = run_rows[0]["run_date"]
PRIOR_RUN_STATUS = run_rows[0]["run_status"]

PRODUCTION_READY_STATUSES = {
    "ANALYSIS_TABLES_READY",
    "HEALTH_RULES_EVALUATED",
}
TEST_READY_STATUSES = {
    "TEST_ANALYSIS_TABLES_READY",
    "TEST_HEALTH_RULES_EVALUATED",
}

if PRIOR_RUN_STATUS not in PRODUCTION_READY_STATUSES and not (
    ALLOW_INCOMPLETE_RUN and PRIOR_RUN_STATUS in TEST_READY_STATUSES
):
    raise RuntimeError(
        f"Run {RUN_ID} has status {PRIOR_RUN_STATUS}. "
        "Production rules require ANALYSIS_TABLES_READY. "
        "Use allow_incomplete_run=true only for the controlled sample test."
    )

IS_TEST_RUN = PRIOR_RUN_STATUS in TEST_READY_STATUSES

print(f"Run ID: {RUN_ID}")
print(f"Run date: {RUN_DATE}")
print(f"Prior run status: {PRIOR_RUN_STATUS}")
print(f"Controlled test mode: {IS_TEST_RUN}")
print(f"Top issues per server: {TOP_ISSUES_PER_SERVER}")


# -----------------------------------------------------------------------------
# 2. Create separate Agent Gold rule and findings tables
# -----------------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_rule_catalog')} (
        rule_id STRING NOT NULL,
        rule_version STRING NOT NULL,
        domain STRING NOT NULL,
        rule_title STRING NOT NULL,
        default_severity STRING NOT NULL,
        rule_description STRING NOT NULL,
        cause_template STRING NOT NULL,
        recommended_action STRING NOT NULL,
        microsoft_reference_url STRING NOT NULL,
        threshold_note STRING,
        threshold_config_json STRING,
        is_enabled BOOLEAN NOT NULL,
        created_ts TIMESTAMP NOT NULL,
        updated_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'gold',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_findings')} (
        finding_id STRING NOT NULL,
        issue_key STRING NOT NULL,
        run_id STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        canonical_server_name STRING NOT NULL,
        rule_id STRING NOT NULL,
        rule_version STRING NOT NULL,
        domain STRING NOT NULL,
        severity STRING NOT NULL,
        priority_score DOUBLE NOT NULL,
        rule_title STRING NOT NULL,
        entity_type STRING,
        entity_name STRING,
        finding_summary STRING NOT NULL,
        likely_cause STRING NOT NULL,
        evidence_json STRING NOT NULL,
        recommended_action STRING NOT NULL,
        microsoft_reference_url STRING NOT NULL,
        threshold_note STRING,
        source_observed_ts TIMESTAMP,
        finding_context STRING NOT NULL,
        finding_status STRING NOT NULL,
        first_detected_ts TIMESTAMP NOT NULL,
        last_detected_ts TIMESTAMP NOT NULL,
        detected_run_count BIGINT NOT NULL,
        created_ts TIMESTAMP NOT NULL,
        updated_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'gold',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_server_health_summary')} (
        run_id STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        canonical_server_name STRING NOT NULL,
        health_status STRING NOT NULL,
        health_score DOUBLE,
        critical_issue_count INT NOT NULL,
        high_issue_count INT NOT NULL,
        medium_issue_count INT NOT NULL,
        low_issue_count INT NOT NULL,
        data_quality_blocker_count INT NOT NULL,
        total_actionable_issue_count INT NOT NULL,
        top_finding_ids ARRAY<STRING> NOT NULL,
        latest_observed_ts TIMESTAMP,
        summary_context STRING NOT NULL,
        evaluated_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'gold',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)


# -----------------------------------------------------------------------------
# 3. Versioned rule catalog with Microsoft sources and threshold ownership
# -----------------------------------------------------------------------------

RULE_VERSION = "2026.08.25.1"
NOW_UTC = datetime.now(timezone.utc)

MS_BACKUP = "https://learn.microsoft.com/en-us/sql/relational-databases/backup-restore/back-up-and-restore-of-sql-server-databases"
MS_CHECKDB = "https://learn.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checkdb-transact-sql"
MS_BACKUP_CHECKSUM = "https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/backup-checksum-default-server-configuration-option"
MS_POLICY = "https://learn.microsoft.com/en-us/sql/relational-databases/policy-based-management/monitor-and-enforce-best-practices-by-using-policy-based-management"
MS_MAXDOP = "https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-the-max-degree-of-parallelism-server-configuration-option"
MS_MEMORY = "https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/server-memory-server-configuration-options"
MS_MEMORY_COUNTER = "https://learn.microsoft.com/en-us/sql/relational-databases/performance-monitor/sql-server-memory-manager-object"
MS_CPU = "https://learn.microsoft.com/en-us/sql/relational-databases/performance-monitor/monitor-cpu-usage"
MS_IO = "https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/performance/troubleshoot-sql-io-performance"
MS_BLOCKING = "https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/performance/understand-resolve-blocking"
MS_VLF = "https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-db-log-info-transact-sql"
MS_SECURITY = "https://learn.microsoft.com/en-us/sql/relational-databases/security/sql-server-security-best-practices"
MS_ADHOC = "https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/optimize-for-ad-hoc-workloads-server-configuration-option"
MS_EVENT_41 = "https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/event-id-41-restart"
MS_STORAGE_EVENTS = "https://learn.microsoft.com/en-us/troubleshoot/windows-server/backup-and-storage/troubleshoot-data-corruption-and-disk-errors"
MS_DEFENDER_EVENTS = "https://learn.microsoft.com/en-us/defender-endpoint/troubleshoot-microsoft-defender-antivirus"
MS_ACCOUNT_LOCKOUT = "https://learn.microsoft.com/en-us/previous-versions/windows/it-pro/windows-10/security/threat-protection/auditing/event-4740"


def threshold_json(**values) -> str:
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


RULES = [
    ("DQ001", "DATA_QUALITY", "SQL diagnostic workbook missing", "CRITICAL", "No SQL diagnostic workbook is available for the server in this run.", "The expected server workbook was not delivered, validated, or ingested.", "Restore the collection/upload process and rerun ingestion before using the Agent diagnosis.", MS_BACKUP, "Collection completeness requirement; not a Microsoft product threshold.", None),
    ("DQ002", "DATA_QUALITY", "SQL diagnostic snapshot is stale", "CRITICAL", "The newest timestamp inside the diagnostic workbook is older than the allowed source age.", "A historical or stale workbook appears to have been uploaded as the current daily diagnostic file.", "Collect a fresh diagnostic workbook from the server, replace the inbox file, and rerun the workflow.", MS_BACKUP, "Agent operational freshness control.", threshold_json(source_stale_days=SOURCE_STALE_DAYS)),
    ("DQ003", "DATA_QUALITY", "Windows Events source is stale", "HIGH", "The newest Windows event in the current source is older than the allowed source age.", "The consolidated Windows Events file was not refreshed with current server events.", "Regenerate the consolidated Windows Events extract, upload it to the inbox, and rerun the workflow.", MS_EVENT_41, "Agent operational freshness control.", threshold_json(source_stale_days=SOURCE_STALE_DAYS)),
    ("DQ004", "DATA_QUALITY", "Required diagnostic worksheet unavailable", "CRITICAL", "An expected diagnostic worksheet is missing or could not be read.", "The workbook structure changed or the collection query failed.", "Review the sheet manifest and quarantine details, correct the collector/export, and rerun ingestion.", MS_BACKUP, "Verified 53-sheet workbook contract.", None),
    ("BK001", "BACKUP_RECOVERY", "Full backup is missing", "CRITICAL", "No successful full backup timestamp is present for the database.", "The database might never have been backed up, backup history may be unavailable, or the backup process is failing.", "Confirm the database protection policy, run a full backup, validate it, and investigate the backup job history.", MS_BACKUP, "Backup frequency is governed by the client's RPO/RTO; this rule identifies a missing baseline full backup.", None),
    ("BK002", "BACKUP_RECOVERY", "Full backup exceeds the operational age", "CRITICAL", "The most recent full backup is older than the configured maximum age.", "The scheduled full backup may have failed, been disabled, or written to an unavailable destination.", "Check the backup job and SQL error log, complete a successful full backup, and verify restore usability.", MS_BACKUP, "Client-configurable operational threshold, not a universal Microsoft interval.", threshold_json(max_hours=FULL_BACKUP_MAX_HOURS)),
    ("BK003", "BACKUP_RECOVERY", "Log backup missing or stale", "HIGH", "A FULL or BULK_LOGGED database has no recent transaction-log backup.", "The log-backup job may be disabled or failing, risking the recovery-point objective and transaction-log growth.", "Restore the transaction-log backup schedule, confirm the log chain, and investigate job or destination failures.", MS_BACKUP, "Client-configurable operational threshold based on the desired RPO.", threshold_json(max_hours=LOG_BACKUP_MAX_HOURS)),
    ("BK004", "BACKUP_RECOVERY", "Integrity check missing or stale", "HIGH", "The last successful DBCC CHECKDB timestamp is missing or older than the configured interval.", "Database consistency checks are not running successfully or their history is unavailable.", "Run DBCC CHECKDB through the approved maintenance process and investigate any reported consistency errors.", MS_CHECKDB, "Client-configurable operational interval; Microsoft recommends regular integrity validation but no single interval for every workload.", threshold_json(max_days=CHECKDB_MAX_DAYS)),
    ("BK005", "BACKUP_RECOVERY", "Windows event reports backup failure", "CRITICAL", "A recent Windows or SQL event explicitly reports a failed backup operation.", "The underlying event often points to destination access, VDI/VSS, storage, or I/O failure; error 3041 is commonly a terminating backup message.", "Review the paired SQL/backup application error, correct the underlying failure, rerun the backup, and validate the result.", MS_BACKUP, "High-confidence event signature.", None),
    ("JB001", "SQL_AGENT", "Critical maintenance job failed", "CRITICAL", "An enabled backup, integrity, or database-maintenance job last ended in failure, retry, or cancellation.", "The maintenance task did not complete successfully; the job step history contains the detailed cause.", "Open SQL Agent history for the failed step, correct the cause, rerun the job, and verify completion.", MS_BACKUP, "High-impact job-name classification plus SQL Agent run status.", None),
    ("JB002", "SQL_AGENT", "Enabled SQL Agent job failed", "HIGH", "An enabled SQL Agent job last ended in failure, retry, or cancellation.", "A job step failed or the execution was interrupted.", "Review SQL Agent job and step history, correct the failing dependency or command, and rerun where appropriate.", MS_BACKUP, "SQL Agent run-status rule; business criticality can be customized later.", None),
    ("SC001", "SECURITY", "xp_cmdshell is enabled", "CRITICAL", "The xp_cmdshell surface-area option is enabled.", "xp_cmdshell starts an operating-system command shell under SQL Server security context and expands attack impact.", "Disable xp_cmdshell unless an approved task requires it; if required, enable only for the task duration and enforce least privilege.", MS_SECURITY, "Microsoft recommends leaving xp_cmdshell disabled.", None),
    ("SC002", "SECURITY", "OLE Automation Procedures is enabled", "HIGH", "OLE Automation Procedures is enabled at the SQL Server instance.", "The option expands the server surface area by allowing OLE automation from T-SQL.", "Confirm a documented business dependency; otherwise disable the option and replace the dependency with a safer supported mechanism.", MS_SECURITY, "Surface-area security review.", None),
    ("SC003", "SECURITY", "Ad Hoc Distributed Queries is enabled", "MEDIUM", "Ad Hoc Distributed Queries is enabled.", "OPENROWSET and OPENDATASOURCE access expands remote-data access paths and should be limited to justified use.", "Validate the requirement and permissions; prefer controlled linked servers for repeatedly accessed data sources.", MS_SECURITY, "Surface-area review; enabled state is not automatically a vulnerability when properly governed.", None),
    ("SC004", "BACKUP_RECOVERY", "Backup checksum default is disabled", "HIGH", "The instance does not enable backup checksums by default.", "Backups can complete without validating page checksums during read or generating a checksum over the backup stream unless each command explicitly requests CHECKSUM.", "Enable backup checksum default after compatibility review, or verify every backup command explicitly uses CHECKSUM.", MS_BACKUP_CHECKSUM, "Microsoft backup checksum capability and integrity control.", None),
    ("PC001", "CONFIGURATION", "MAXDOP exceeds Microsoft topology guidance", "HIGH", "The configured MAXDOP exceeds the recommendation derived from logical processors and NUMA topology.", "Excessive parallelism can place workers from one parallel query across NUMA boundaries or consume too many schedulers.", "Review workload characteristics and set MAXDOP at or below the topology-based Microsoft recommendation.", MS_MAXDOP, "Direct Microsoft processor/NUMA guidance.", None),
    ("PC002", "MEMORY", "Max server memory leaves insufficient OS headroom", "HIGH", "The configured max server memory leaves very little physical memory for Windows and allocations outside the buffer pool.", "SQL Server memory was configured too close to total physical memory.", "Recalculate max server memory after reserving memory for Windows, non-buffer-pool allocations, other instances, and applications.", MS_MEMORY, "Operational guardrail: at least 4 GB and 5% headroom; final sizing must be workload-tested.", threshold_json(minimum_headroom_mb=4096, minimum_headroom_percent=5)),
    ("PC003", "MEMORY", "Large ad hoc plan cache is not optimized", "MEDIUM", "Many or large ad hoc plans are present while optimize for ad hoc workloads is disabled.", "Single-use compiled plans may be consuming avoidable plan-cache memory.", "Confirm the workload is dominated by single-use ad hoc batches, then consider enabling optimize for ad hoc workloads and improve parameterization.", MS_ADHOC, "Conditional Microsoft guidance; enable only after confirming single-use plan behavior.", threshold_json(minimum_rows=100, minimum_plan_cache_mb=512)),
    ("DB001", "DATABASE", "Database is not ONLINE", "CRITICAL", "A database state is not ONLINE.", "The database might be offline, restoring, recovering, suspect, or otherwise unavailable.", "Review the database state and SQL error log immediately; restore service using the appropriate recovery procedure.", MS_POLICY, "Availability state rule.", None),
    ("DB002", "DATABASE", "PAGE_VERIFY is not CHECKSUM", "HIGH", "The database PAGE_VERIFY option is not CHECKSUM.", "Page-level I/O corruption detection is weaker than the Microsoft best-practice setting.", "Set PAGE_VERIFY CHECKSUM during an approved change and validate database integrity.", MS_POLICY, "Direct Microsoft Policy-Based Management best-practice rule.", None),
    ("DB003", "DATABASE", "AUTO_CLOSE or AUTO_SHRINK is enabled", "HIGH", "A user database has AUTO_CLOSE or AUTO_SHRINK enabled.", "AUTO_CLOSE repeatedly releases/reacquires resources, while AUTO_SHRINK can cause churn and fragmentation.", "Disable AUTO_CLOSE and AUTO_SHRINK unless a documented exceptional use case exists.", MS_POLICY, "Direct Microsoft Policy-Based Management best-practice rules.", None),
    ("DB004", "DATABASE", "Transaction log space is critically utilized", "HIGH", "Database log usage is at or above the configured high threshold.", "Log reuse may be blocked, log backups may be missing, or a long transaction/availability dependency may be preventing truncation.", "Inspect log_reuse_wait_desc, active transactions, backup health, and available disk space before taking corrective action.", MS_BACKUP, "Operational utilization threshold.", threshold_json(high_percent=80, critical_percent=90)),
    ("DB005", "DATABASE", "Excessive virtual log file count", "HIGH", "A database has more than 100 virtual log files.", "Frequent small transaction-log growth increments can create excessive VLFs and lengthen startup, restore, and recovery.", "Plan a controlled VLF remediation and configure appropriate fixed log-growth increments; do not shrink routinely.", MS_VLF, "Microsoft example flags databases with more than 100 VLFs.", threshold_json(high_count=100, critical_count=1000)),
    ("ST001", "STORAGE", "Volume free space is low", "HIGH", "A SQL Server volume has fallen below the configured free-space threshold.", "Database, log, backup, tempdb, or application files may be consuming the volume faster than planned.", "Identify growth drivers, protect immediate free space, verify autogrowth destinations, and expand or clean the volume through approved procedures.", MS_IO, "Client-configurable capacity threshold.", threshold_json(high_percent=DISK_HIGH_FREE_PERCENT, critical_percent=DISK_CRITICAL_FREE_PERCENT)),
    ("IO001", "IO", "SQL Server I/O latency is high", "HIGH", "Measured drive or database-file latency exceeds the configured operational threshold.", "Storage contention, overloaded paths, throttling, driver/HBA issues, or competing workloads may be delaying SQL I/O.", "Correlate file and drive latency with PAGEIOLATCH/WRITELOG waits and Windows storage events, then investigate the storage path.", MS_IO, "Operational threshold; baseline against the actual storage SLA.", threshold_json(high_ms=IO_HIGH_LATENCY_MS, critical_ms=IO_CRITICAL_LATENCY_MS)),
    ("MEM001", "MEMORY", "Memory grants are pending", "HIGH", "Queries are waiting for workspace memory grants.", "Concurrent sorts, hashes, index operations, poor estimates, or memory pressure are exhausting available query-workspace memory.", "Identify waiting queries and RESOURCE_SEMAPHORE pressure; tune memory-intensive plans and validate instance memory configuration.", MS_MEMORY_COUNTER, "Any pending grant is actionable when persistent; magnitude raises priority.", None),
    ("MEM002", "MEMORY", "SQL Server reports process memory pressure", "CRITICAL", "SQL Server reports low physical or virtual process memory.", "The operating system or SQL Server memory manager cannot satisfy current memory requirements.", "Investigate OS memory pressure, max server memory, other consumers, query grants, and paging immediately.", MS_MEMORY, "Direct SQL process-memory pressure flags.", None),
    ("MEM003", "MEMORY", "Available physical memory is low", "HIGH", "Available physical memory is below the operational percentage threshold.", "SQL Server, other processes, or the hypervisor are consuming most host memory.", "Confirm sustained pressure, paging, host/VM memory allocation, and max server memory before resizing or reconfiguration.", MS_MEMORY, "Operational guardrail: High below 10%, Critical below 5%.", threshold_json(high_percent=10, critical_percent=5)),
    ("CPU001", "CPU", "Sustained SQL Server CPU is high", "HIGH", "Average SQL Server process CPU across the captured history exceeds the configured threshold.", "Common causes include high logical reads, inefficient queries, missing indexes, stale statistics, plan regressions, or workload growth.", "Confirm SQL Server is the CPU consumer, then inspect the top worker-time and logical-read queries before changing capacity.", MS_CPU, "Microsoft identifies consistent 80-90% CPU as a capacity/performance concern.", threshold_json(high_percent=CPU_HIGH_PERCENT, critical_percent=CPU_CRITICAL_PERCENT)),
    ("BLK001", "BLOCKING", "Long-running blocking detected", "HIGH", "A captured blocking chain exceeds the configured duration.", "A transaction is holding incompatible locks while another session waits; long transactions or poor access patterns are common causes.", "Capture the blocker transaction and execution context, protect data integrity, then correct the transaction/query/access pattern.", MS_BLOCKING, "Operational duration threshold; Microsoft blocked-process reporting begins at 5 seconds.", threshold_json(high_seconds=BLOCKING_HIGH_SECONDS, critical_seconds=BLOCKING_CRITICAL_SECONDS)),
    ("WN001", "WINDOWS_STORAGE", "Windows reports storage timeout or corruption", "CRITICAL", "A recent high-confidence Windows storage event indicates timeout, retry, reset, corruption, or device loss.", "The storage subsystem may be overloaded or experiencing an HBA, path, driver, firmware, media, or connectivity problem.", "Review surrounding System events and storage diagnostics immediately; validate disk, path, controller, driver, firmware, and SAN health.", MS_STORAGE_EVENTS, "High-confidence provider and event-ID signatures.", None),
    ("WN002", "WINDOWS_AVAILABILITY", "Unexpected Windows restart detected", "CRITICAL", "Windows recorded an unexpected shutdown or restart.", "Power interruption, Stop error, hardware failure, or an unresponsive system prevented a clean shutdown.", "Correlate Event 41/6008 with bugcheck, WHEA, power, driver, and preceding application/storage events.", MS_EVENT_41, "Microsoft Event 41/6008 semantics.", None),
    ("WN003", "WINDOWS_SECURITY", "Microsoft Defender protection issue", "CRITICAL", "Microsoft Defender detected malware, failed remediation, or reported disabled protection.", "Malware or potentially unwanted software was detected, remediation failed, or protection was disabled.", "Follow the security incident process, inspect Defender details, isolate where required, confirm remediation, and rescan.", MS_DEFENDER_EVENTS, "Microsoft Defender event signatures 1116, 1119, and 5010.", None),
    ("WN004", "WINDOWS_SECURITY", "Repeated authentication or account-lockout failures", "HIGH", "Recent Windows Security events show account lockout or repeated failed logons.", "Stale credentials, service accounts, scheduled tasks, password spraying, or unauthorized access attempts may be responsible.", "Identify the account and source host, validate legitimacy, rotate or repair credentials, and investigate malicious patterns.", MS_ACCOUNT_LOCKOUT, "Event 4740 or repeated 4625 events.", threshold_json(failed_logon_minimum_occurrences=5)),
    ("WN005", "WINDOWS_AVAILABILITY", "Windows service terminated unexpectedly", "HIGH", "The Service Control Manager reports an unexpected service termination.", "The service crashed, was killed, or failed because of a dependency, resource, or application error.", "Identify the service, review its application logs and dump data, correct the cause, and confirm stable restart.", MS_EVENT_41, "Service Control Manager events 7031 and 7034.", None),
    ("WN006", "WINDOWS_SECURITY", "Kerberos SPN or service-account mismatch", "HIGH", "Kerberos reported KRB_AP_ERR_MODIFIED for the server.", "The SPN may be registered to the wrong account, a duplicate SPN may exist, or the service/KDC passwords differ.", "Validate SPN ownership and duplicates, confirm the service identity, and reconcile the service-account password with Active Directory.", MS_ACCOUNT_LOCKOUT, "High-confidence Kerberos provider Event 4 signature.", None),
    ("WN007", "WINDOWS_SECURITY", "NTLM authentication detected", "MEDIUM", "Windows recorded NTLM use and identified it as a weaker authentication mechanism.", "An application, service, name-resolution issue, or SPN problem is preventing Kerberos authentication.", "Identify the NTLM client/application, correct Kerberos/SPN/name-resolution issues, and enforce Extended Protection where NTLM remains necessary.", MS_SECURITY, "LsaSrv Event 6038; review before remediation because legacy dependencies may exist.", None),
    ("WN008", "WINDOWS_BACKUP_STORAGE", "VSS shadow-copy storage limit reached", "HIGH", "Volsnap reports that a shadow copy was aborted because shadow-copy storage could not grow.", "The configured shadow-copy storage limit or underlying free space is insufficient.", "Review VSS allocations and volume capacity, correct the limit or free-space issue, then rerun and validate the affected backup/snapshot.", MS_STORAGE_EVENTS, "Volsnap Event 36 signature.", None),
    ("WN009", "WINDOWS_SECURITY", "Repeated Group Policy processing failure", "HIGH", "Repeated Group Policy events show that policy retrieval or application failed.", "DNS, domain-controller reachability, authentication, permissions, or a policy extension may be failing.", "Review the event error details, DNS and DC connectivity, then correct the failing policy extension and confirm a successful refresh.", MS_ACCOUNT_LOCKOUT, "Group Policy events 1030, 1054, or 1085 with repeated occurrences.", threshold_json(minimum_occurrences=3)),
    ("WN010", "WINDOWS_SECURITY", "Repeated Schannel TLS handshake failure", "HIGH", "Repeated Schannel events show TLS handshake or certificate negotiation failure.", "Protocol/cipher mismatch, certificate trust/expiry, client-authentication, or incompatible legacy software may be involved.", "Inspect the Schannel event details and peer, validate certificates and TLS policy, and correct the incompatible endpoint.", MS_SECURITY, "Schannel events 36874 or 36888 with repeated occurrences.", threshold_json(minimum_occurrences=5)),
]

rule_schema = StructType(
    [
        StructField("rule_id", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("rule_title", StringType(), False),
        StructField("default_severity", StringType(), False),
        StructField("rule_description", StringType(), False),
        StructField("cause_template", StringType(), False),
        StructField("recommended_action", StringType(), False),
        StructField("microsoft_reference_url", StringType(), False),
        StructField("threshold_note", StringType(), True),
        StructField("threshold_config_json", StringType(), True),
    ]
)

rule_catalog_df = (
    spark.createDataFrame(RULES, rule_schema)
    .withColumn("rule_version", F.lit(RULE_VERSION))
    .withColumn("is_enabled", F.lit(True).cast(BooleanType()))
    .withColumn("created_ts", F.lit(NOW_UTC).cast(TimestampType()))
    .withColumn("updated_ts", F.lit(NOW_UTC).cast(TimestampType()))
    .select(
        "rule_id",
        "rule_version",
        "domain",
        "rule_title",
        "default_severity",
        "rule_description",
        "cause_template",
        "recommended_action",
        "microsoft_reference_url",
        "threshold_note",
        "threshold_config_json",
        "is_enabled",
        "created_ts",
        "updated_ts",
    )
)

rule_catalog_df.createOrReplaceTempView("_agent_rule_catalog_source")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_rule_catalog')} AS target
    USING _agent_rule_catalog_source AS source
       ON target.rule_id = source.rule_id
    WHEN MATCHED THEN UPDATE SET
        target.rule_version = source.rule_version,
        target.domain = source.domain,
        target.rule_title = source.rule_title,
        target.default_severity = source.default_severity,
        target.rule_description = source.rule_description,
        target.cause_template = source.cause_template,
        target.recommended_action = source.recommended_action,
        target.microsoft_reference_url = source.microsoft_reference_url,
        target.threshold_note = source.threshold_note,
        target.threshold_config_json = source.threshold_config_json,
        target.is_enabled = source.is_enabled,
        target.updated_ts = source.updated_ts
    WHEN NOT MATCHED THEN INSERT *
    """
)

spark.catalog.dropTempView("_agent_rule_catalog_source")


# -----------------------------------------------------------------------------
# 4. Current-run data context and point-in-time controls
# -----------------------------------------------------------------------------

inventory_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_server_daily_inventory")
    .where(F.col("run_id") == RUN_ID)
)

if inventory_df.limit(1).count() == 0:
    raise RuntimeError(f"No Agent server inventory exists for {RUN_ID}. Run notebook 04.")

inventory_df.createOrReplaceTempView("_agent_inventory_current")

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW _agent_sql_context AS
    WITH sql_observed AS (
        SELECT
            canonical_server_name,
            MAX(observed_ts) AS sql_latest_observed_ts
        FROM {table_name('agent_sql_rows_silver')}
        WHERE run_id = '{RUN_ID}'
        GROUP BY canonical_server_name
    ),
    windows_observed AS (
        SELECT
            canonical_server_name,
            MAX(event_time) AS windows_latest_observed_ts
        FROM {table_name('agent_windows_events_silver')}
        WHERE run_id = '{RUN_ID}'
        GROUP BY canonical_server_name
    )
    SELECT
        inventory.*,
        sql_observed.sql_latest_observed_ts,
        windows_observed.windows_latest_observed_ts,
        CASE
            WHEN {str(IS_TEST_RUN).lower()} THEN coalesce(
                sql_observed.sql_latest_observed_ts,
                cast(inventory.snapshot_date AS TIMESTAMP)
            )
            ELSE cast(inventory.snapshot_date AS TIMESTAMP)
        END AS evaluation_ts,
        datediff(
            inventory.snapshot_date,
            to_date(sql_observed.sql_latest_observed_ts)
        ) AS sql_source_age_days,
        datediff(
            inventory.snapshot_date,
            to_date(windows_observed.windows_latest_observed_ts)
        ) AS windows_source_age_days,
        CASE
            WHEN {str(IS_TEST_RUN).lower()} THEN true
            WHEN sql_observed.sql_latest_observed_ts IS NULL THEN false
            WHEN datediff(inventory.snapshot_date, to_date(sql_observed.sql_latest_observed_ts)) <= {SOURCE_STALE_DAYS} THEN true
            ELSE false
        END AS may_evaluate_sql_rules,
        CASE
            WHEN {str(IS_TEST_RUN).lower()} THEN true
            WHEN windows_observed.windows_latest_observed_ts IS NULL THEN false
            WHEN datediff(inventory.snapshot_date, to_date(windows_observed.windows_latest_observed_ts)) <= {SOURCE_STALE_DAYS} THEN true
            ELSE false
        END AS may_evaluate_windows_rules
    FROM _agent_inventory_current AS inventory
    LEFT JOIN sql_observed
        ON inventory.canonical_server_name = sql_observed.canonical_server_name
    LEFT JOIN windows_observed
        ON inventory.canonical_server_name = windows_observed.canonical_server_name
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW _agent_windows_recent_grouped AS
    SELECT
        events.canonical_server_name,
        context.snapshot_date,
        events.provider_name,
        events.event_id,
        events.severity_class,
        MAX(events.event_time) AS latest_event_time,
        SUM(events.occurrence_count) AS source_occurrences,
        COUNT(*) AS unique_events,
        MAX(events.message) AS sample_message,
        context.may_evaluate_windows_rules
    FROM {table_name('v_agent_recent_windows_events_silver')} AS events
    INNER JOIN _agent_sql_context AS context
        ON events.canonical_server_name = context.canonical_server_name
    GROUP BY
        events.canonical_server_name,
        context.snapshot_date,
        events.provider_name,
        events.event_id,
        events.severity_class,
        context.may_evaluate_windows_rules
    """
)


# -----------------------------------------------------------------------------
# 5. Deterministic candidate findings
# -----------------------------------------------------------------------------

candidate_sql_blocks = []


def add_candidate_sql(sql_text: str) -> None:
    candidate_sql_blocks.append(sql_text.strip())


# Data completeness and freshness.
add_candidate_sql(
    """
    SELECT 'DQ001' AS rule_id, canonical_server_name, snapshot_date,
           'SERVER' AS entity_type, canonical_server_name AS entity_name,
           'CRITICAL' AS severity, 100.0 AS priority_score,
           concat('No SQL diagnostic workbook was ingested for ', canonical_server_name, '.') AS finding_summary,
           'The expected current server workbook is absent from the validated and ingested inputs.' AS likely_cause,
           to_json(named_struct('has_sql_workbook', has_sql_workbook, 'inventory_status', inventory_status)) AS evidence_json,
           latest_observed_ts AS source_observed_ts
    FROM _agent_sql_context
    WHERE has_sql_workbook = false
    """
)

add_candidate_sql(
    f"""
    SELECT 'DQ002' AS rule_id, canonical_server_name, snapshot_date,
           'SERVER' AS entity_type, canonical_server_name AS entity_name,
           'CRITICAL' AS severity, 100.0 AS priority_score,
           concat('SQL diagnostic data is ', sql_source_age_days, ' days older than the run date.') AS finding_summary,
           'The timestamps inside the workbook indicate historical rather than current diagnostic data.' AS likely_cause,
           to_json(named_struct('sql_latest_observed_ts', sql_latest_observed_ts, 'source_age_days', sql_source_age_days, 'allowed_days', {SOURCE_STALE_DAYS})) AS evidence_json,
           sql_latest_observed_ts AS source_observed_ts
    FROM _agent_sql_context
    WHERE has_sql_workbook = true
      AND (sql_latest_observed_ts IS NULL OR sql_source_age_days > {SOURCE_STALE_DAYS})
    """
)

add_candidate_sql(
    f"""
    WITH windows_freshness AS (
        SELECT canonical_server_name, MAX(event_time) AS latest_windows_event_ts
        FROM {table_name('agent_windows_events_silver')}
        WHERE run_id = '{RUN_ID}'
        GROUP BY canonical_server_name
    )
    SELECT 'DQ003' AS rule_id, context.canonical_server_name, context.snapshot_date,
           'SERVER' AS entity_type, context.canonical_server_name AS entity_name,
           'HIGH' AS severity, 80.0 AS priority_score,
           concat('Windows Events data is ', datediff(context.snapshot_date, to_date(w.latest_windows_event_ts)), ' days older than the run date.') AS finding_summary,
           'The current consolidated Windows Events source does not contain recent events for this server.' AS likely_cause,
           to_json(named_struct('latest_windows_event_ts', w.latest_windows_event_ts, 'allowed_days', {SOURCE_STALE_DAYS})) AS evidence_json,
           w.latest_windows_event_ts AS source_observed_ts
    FROM _agent_sql_context AS context
    INNER JOIN windows_freshness AS w
        ON context.canonical_server_name = w.canonical_server_name
    WHERE datediff(context.snapshot_date, to_date(w.latest_windows_event_ts)) > {SOURCE_STALE_DAYS}
    """
)

add_candidate_sql(
    f"""
    SELECT 'DQ004' AS rule_id, manifest.canonical_server_name, context.snapshot_date,
           'WORKSHEET' AS entity_type, manifest.sheet_name AS entity_name,
           'CRITICAL' AS severity, 100.0 AS priority_score,
           concat('Required worksheet ', manifest.sheet_name, ' has status ', manifest.sheet_status, '.') AS finding_summary,
           coalesce(manifest.error_message, 'The expected worksheet is missing or unreadable.') AS likely_cause,
           to_json(named_struct('sheet_ordinal', manifest.sheet_ordinal, 'sheet_name', manifest.sheet_name, 'sheet_status', manifest.sheet_status, 'error_message', manifest.error_message)) AS evidence_json,
           manifest.processing_completed_ts AS source_observed_ts
    FROM {table_name('agent_sheet_manifest')} AS manifest
    INNER JOIN _agent_sql_context AS context
        ON manifest.canonical_server_name = context.canonical_server_name
    WHERE manifest.run_id = '{RUN_ID}'
      AND manifest.sheet_status IN ('MISSING_EXPECTED', 'READ_ERROR', 'EMPTY')
    """
)

# Backup and integrity rules.
add_candidate_sql(
    f"""
    SELECT 'BK001' AS rule_id, backup.canonical_server_name, backup.snapshot_date,
           'DATABASE' AS entity_type, backup.database_name AS entity_name,
           'CRITICAL' AS severity, 100.0 AS priority_score,
           concat('Database ', backup.database_name, ' has no recorded successful full backup.') AS finding_summary,
           'No full-backup timestamp is present in the SQL backup history returned by the collector.' AS likely_cause,
           to_json(named_struct('database', backup.database_name, 'recovery_model', backup.recovery_model, 'last_full_backup_ts', backup.last_full_backup_ts)) AS evidence_json,
           backup.last_full_backup_ts AS source_observed_ts
    FROM {table_name('v_agent_backup_health')} AS backup
    INNER JOIN _agent_sql_context AS context
        ON backup.canonical_server_name = context.canonical_server_name
       AND backup.snapshot_date = context.snapshot_date
    WHERE backup.run_id = '{RUN_ID}'
      AND context.may_evaluate_sql_rules = true
      AND backup.last_full_backup_ts IS NULL
    """
)

add_candidate_sql(
    f"""
    SELECT 'BK002' AS rule_id, backup.canonical_server_name, backup.snapshot_date,
           'DATABASE' AS entity_type, backup.database_name AS entity_name,
           'CRITICAL' AS severity, 98.0 AS priority_score,
           concat('Full backup for ', backup.database_name, ' is ', cast(round((unix_timestamp(context.evaluation_ts) - unix_timestamp(backup.last_full_backup_ts)) / 3600.0, 1) AS STRING), ' hours old.') AS finding_summary,
           'The most recent full backup is beyond the configured operational protection interval.' AS likely_cause,
           to_json(named_struct('database', backup.database_name, 'last_full_backup_ts', backup.last_full_backup_ts, 'evaluation_ts', context.evaluation_ts, 'maximum_hours', {FULL_BACKUP_MAX_HOURS})) AS evidence_json,
           backup.last_full_backup_ts AS source_observed_ts
    FROM {table_name('v_agent_backup_health')} AS backup
    INNER JOIN _agent_sql_context AS context
        ON backup.canonical_server_name = context.canonical_server_name
       AND backup.snapshot_date = context.snapshot_date
    WHERE backup.run_id = '{RUN_ID}'
      AND context.may_evaluate_sql_rules = true
      AND backup.last_full_backup_ts IS NOT NULL
      AND (unix_timestamp(context.evaluation_ts) - unix_timestamp(backup.last_full_backup_ts)) / 3600.0 > {FULL_BACKUP_MAX_HOURS}
    """
)

add_candidate_sql(
    f"""
    SELECT 'BK003' AS rule_id, backup.canonical_server_name, backup.snapshot_date,
           'DATABASE' AS entity_type, backup.database_name AS entity_name,
           'HIGH' AS severity, 88.0 AS priority_score,
           CASE
               WHEN backup.last_log_backup_ts IS NULL THEN concat('Database ', backup.database_name, ' has no recorded log backup.')
               ELSE concat('Log backup for ', backup.database_name, ' is ', cast(round((unix_timestamp(context.evaluation_ts) - unix_timestamp(backup.last_log_backup_ts)) / 3600.0, 1) AS STRING), ' hours old.')
           END AS finding_summary,
           'The transaction-log backup schedule is missing, stale, or failing for a database that requires log backups.' AS likely_cause,
           to_json(named_struct('database', backup.database_name, 'recovery_model', backup.recovery_model, 'last_log_backup_ts', backup.last_log_backup_ts, 'maximum_hours', {LOG_BACKUP_MAX_HOURS})) AS evidence_json,
           backup.last_log_backup_ts AS source_observed_ts
    FROM {table_name('v_agent_backup_health')} AS backup
    INNER JOIN _agent_sql_context AS context
        ON backup.canonical_server_name = context.canonical_server_name
       AND backup.snapshot_date = context.snapshot_date
    WHERE backup.run_id = '{RUN_ID}'
      AND context.may_evaluate_sql_rules = true
      AND upper(backup.recovery_model) IN ('FULL', 'BULK_LOGGED')
      AND (
          backup.last_log_backup_ts IS NULL
          OR (unix_timestamp(context.evaluation_ts) - unix_timestamp(backup.last_log_backup_ts)) / 3600.0 > {LOG_BACKUP_MAX_HOURS}
      )
    """
)

add_candidate_sql(
    f"""
    SELECT 'BK004' AS rule_id, backup.canonical_server_name, backup.snapshot_date,
           'DATABASE' AS entity_type, backup.database_name AS entity_name,
           'HIGH' AS severity, 84.0 AS priority_score,
           CASE
               WHEN backup.last_good_checkdb_ts IS NULL THEN concat('Database ', backup.database_name, ' has no recorded successful CHECKDB.')
               ELSE concat('Last successful CHECKDB for ', backup.database_name, ' is ', datediff(to_date(context.evaluation_ts), to_date(backup.last_good_checkdb_ts)), ' days old.')
           END AS finding_summary,
           'The integrity-check history is missing or outside the configured maintenance interval.' AS likely_cause,
           to_json(named_struct('database', backup.database_name, 'last_good_checkdb_ts', backup.last_good_checkdb_ts, 'maximum_days', {CHECKDB_MAX_DAYS})) AS evidence_json,
           backup.last_good_checkdb_ts AS source_observed_ts
    FROM {table_name('v_agent_backup_health')} AS backup
    INNER JOIN _agent_sql_context AS context
        ON backup.canonical_server_name = context.canonical_server_name
       AND backup.snapshot_date = context.snapshot_date
    WHERE backup.run_id = '{RUN_ID}'
      AND context.may_evaluate_sql_rules = true
      AND (
          backup.last_good_checkdb_ts IS NULL
          OR datediff(to_date(context.evaluation_ts), to_date(backup.last_good_checkdb_ts)) > {CHECKDB_MAX_DAYS}
      )
    """
)

# SQL Agent failures. Critical maintenance jobs are separated from other jobs.
critical_job_pattern = "backup|checkdb|integrity|dbcc|database consistency|log backup"
add_candidate_sql(
    f"""
    SELECT 'JB001' AS rule_id, job.canonical_server_name, job.snapshot_date,
           'SQL_AGENT_JOB' AS entity_type, job.job_name AS entity_name,
           'CRITICAL' AS severity, 96.0 AS priority_score,
           concat('Critical maintenance job ', job.job_name, ' last ended as ', job.run_status_desc, '.') AS finding_summary,
           'The most recent recorded execution did not complete successfully.' AS likely_cause,
           to_json(named_struct('job_name', job.job_name, 'run_status', job.run_status, 'run_status_desc', job.run_status_desc, 'last_start_ts', job.last_start_ts, 'schedule_name', job.schedule_name)) AS evidence_json,
           job.last_start_ts AS source_observed_ts
    FROM {table_name('v_agent_job_health')} AS job
    INNER JOIN _agent_sql_context AS context
        ON job.canonical_server_name = context.canonical_server_name
       AND job.snapshot_date = context.snapshot_date
    WHERE job.run_id = '{RUN_ID}'
      AND context.may_evaluate_sql_rules = true
      AND job.job_enabled = 1
      AND job.run_status IN (0, 2, 3)
      AND lower(job.job_name) RLIKE '{critical_job_pattern}'
    """
)

add_candidate_sql(
    f"""
    SELECT 'JB002' AS rule_id, job.canonical_server_name, job.snapshot_date,
           'SQL_AGENT_JOB' AS entity_type, job.job_name AS entity_name,
           'HIGH' AS severity, 78.0 AS priority_score,
           concat('Enabled SQL Agent job ', job.job_name, ' last ended as ', job.run_status_desc, '.') AS finding_summary,
           'The most recent recorded execution did not complete successfully.' AS likely_cause,
           to_json(named_struct('job_name', job.job_name, 'run_status', job.run_status, 'run_status_desc', job.run_status_desc, 'last_start_ts', job.last_start_ts, 'schedule_name', job.schedule_name)) AS evidence_json,
           job.last_start_ts AS source_observed_ts
    FROM {table_name('v_agent_job_health')} AS job
    INNER JOIN _agent_sql_context AS context
        ON job.canonical_server_name = context.canonical_server_name
       AND job.snapshot_date = context.snapshot_date
    WHERE job.run_id = '{RUN_ID}'
      AND context.may_evaluate_sql_rules = true
      AND job.job_enabled = 1
      AND job.run_status IN (0, 2, 3)
      AND NOT (lower(job.job_name) RLIKE '{critical_job_pattern}')
    """
)

# Security and server configuration.
config_rules = [
    ("SC001", "xp_cmdshell", "CRITICAL", 100.0, "xp_cmdshell is enabled on the SQL Server instance.", "The operating-system command shell surface area is enabled."),
    ("SC002", "ole automation procedures", "HIGH", 82.0, "OLE Automation Procedures is enabled on the SQL Server instance.", "The OLE automation surface area is enabled."),
    ("SC003", "ad hoc distributed queries", "MEDIUM", 55.0, "Ad Hoc Distributed Queries is enabled on the SQL Server instance.", "Remote OLE DB access through OPENROWSET or OPENDATASOURCE is enabled."),
    ("SC004", "backup checksum default", "HIGH", 86.0, "Backup checksum default is disabled on the SQL Server instance.", "Backups do not automatically request checksum validation unless the command specifies it."),
]

for rule_id, config_name, severity, score, summary, cause in config_rules:
    add_candidate_sql(
        f"""
        SELECT '{rule_id}' AS rule_id, config.canonical_server_name, config.snapshot_date,
               'CONFIGURATION' AS entity_type, config.configuration_name AS entity_name,
               '{severity}' AS severity, {score} AS priority_score,
               '{summary}' AS finding_summary,
               '{cause}' AS likely_cause,
               to_json(named_struct('configuration_name', config.configuration_name, 'configured_value', config.configured_value, 'value_in_use', config.value_in_use)) AS evidence_json,
               cast(config.snapshot_date AS TIMESTAMP) AS source_observed_ts
        FROM {table_name('v_agent_configuration_health')} AS config
        WHERE config.run_id = '{RUN_ID}'
          AND lower(config.configuration_name) = '{config_name}'
          AND config.value_in_use = {0 if rule_id == 'SC004' else 1}
        """
    )

add_candidate_sql(
    f"""
    WITH hardware AS (
        SELECT canonical_server_name, snapshot_date,
               try_cast(row_values['Logical CPU Count'] AS INT) AS logical_cpu_count,
               try_cast(row_values['numa_node_count'] AS INT) AS numa_node_count
        FROM {table_name('agent_sql_rows_silver')}
        WHERE run_id = '{RUN_ID}' AND sheet_name = '17-Hardware Info'
    ), maxdop AS (
        SELECT canonical_server_name, snapshot_date, value_in_use AS configured_maxdop
        FROM {table_name('v_agent_configuration_health')}
        WHERE run_id = '{RUN_ID}' AND lower(configuration_name) = 'max degree of parallelism'
    ), recommendation AS (
        SELECT h.*, m.configured_maxdop,
               CASE
                   WHEN h.numa_node_count <= 1 AND h.logical_cpu_count <= 8 THEN h.logical_cpu_count
                   WHEN h.numa_node_count <= 1 THEN 8
                   WHEN h.logical_cpu_count / h.numa_node_count <= 16 THEN cast(floor(h.logical_cpu_count / h.numa_node_count) AS INT)
                   ELSE least(16, cast(floor((h.logical_cpu_count / h.numa_node_count) / 2) AS INT))
               END AS recommended_maxdop
        FROM hardware h INNER JOIN maxdop m
          ON h.canonical_server_name = m.canonical_server_name AND h.snapshot_date = m.snapshot_date
    )
    SELECT 'PC001' AS rule_id, canonical_server_name, snapshot_date,
           'CONFIGURATION' AS entity_type, 'max degree of parallelism' AS entity_name,
           'HIGH' AS severity, 78.0 AS priority_score,
           concat('MAXDOP ', cast(configured_maxdop AS STRING), ' exceeds the topology recommendation ', cast(recommended_maxdop AS STRING), '.') AS finding_summary,
           'The configured degree of parallelism exceeds the value derived from logical CPU and NUMA topology.' AS likely_cause,
           to_json(named_struct('configured_maxdop', configured_maxdop, 'recommended_maxdop', recommended_maxdop, 'logical_cpu_count', logical_cpu_count, 'numa_node_count', numa_node_count)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM recommendation
    WHERE configured_maxdop > recommended_maxdop OR configured_maxdop = 0
    """
)

add_candidate_sql(
    f"""
    WITH hardware AS (
        SELECT canonical_server_name, snapshot_date,
               try_cast(row_values['Physical Memory (MB)'] AS DOUBLE) AS physical_memory_mb
        FROM {table_name('agent_sql_rows_silver')}
        WHERE run_id = '{RUN_ID}' AND sheet_name = '17-Hardware Info'
    ), memory_config AS (
        SELECT canonical_server_name, snapshot_date, value_in_use AS max_server_memory_mb
        FROM {table_name('v_agent_configuration_health')}
        WHERE run_id = '{RUN_ID}' AND lower(configuration_name) = 'max server memory (mb)'
    )
    SELECT 'PC002' AS rule_id, h.canonical_server_name, h.snapshot_date,
           'CONFIGURATION' AS entity_type, 'max server memory (MB)' AS entity_name,
           'HIGH' AS severity, 82.0 AS priority_score,
           concat('Max server memory leaves ', cast(round(h.physical_memory_mb - m.max_server_memory_mb, 0) AS STRING), ' MB before non-buffer-pool and OS requirements.') AS finding_summary,
           'The configured SQL Server memory ceiling is too close to total physical memory.' AS likely_cause,
           to_json(named_struct('physical_memory_mb', h.physical_memory_mb, 'max_server_memory_mb', m.max_server_memory_mb, 'raw_headroom_mb', h.physical_memory_mb - m.max_server_memory_mb)) AS evidence_json,
           cast(h.snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM hardware h INNER JOIN memory_config m
      ON h.canonical_server_name = m.canonical_server_name AND h.snapshot_date = m.snapshot_date
    WHERE h.physical_memory_mb - m.max_server_memory_mb < 4096
       OR (h.physical_memory_mb - m.max_server_memory_mb) / h.physical_memory_mb * 100 < 5
    """
)

add_candidate_sql(
    f"""
    WITH adhoc AS (
        SELECT canonical_server_name, snapshot_date,
               COUNT(*) AS adhoc_plan_count,
               SUM(try_cast(row_values['Plan Size in KB'] AS DOUBLE)) / 1024.0 AS adhoc_plan_mb
        FROM {table_name('agent_sql_rows_silver')}
        WHERE run_id = '{RUN_ID}' AND sheet_name = '50-Ad hoc Queries'
        GROUP BY canonical_server_name, snapshot_date
    ), setting AS (
        SELECT canonical_server_name, snapshot_date, value_in_use
        FROM {table_name('v_agent_configuration_health')}
        WHERE run_id = '{RUN_ID}' AND lower(configuration_name) = 'optimize for ad hoc workloads'
    )
    SELECT 'PC003' AS rule_id, a.canonical_server_name, a.snapshot_date,
           'CONFIGURATION' AS entity_type, 'optimize for ad hoc workloads' AS entity_name,
           'MEDIUM' AS severity, 58.0 AS priority_score,
           concat(cast(a.adhoc_plan_count AS STRING), ' ad hoc plans use approximately ', cast(round(a.adhoc_plan_mb, 1) AS STRING), ' MB while optimization is disabled.') AS finding_summary,
           'The plan cache contains a significant ad hoc workload and stores full plans on first execution.' AS likely_cause,
           to_json(named_struct('adhoc_plan_count', a.adhoc_plan_count, 'adhoc_plan_mb', a.adhoc_plan_mb, 'setting_value', s.value_in_use)) AS evidence_json,
           cast(a.snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM adhoc a INNER JOIN setting s
      ON a.canonical_server_name = s.canonical_server_name AND a.snapshot_date = s.snapshot_date
    WHERE s.value_in_use = 0 AND (a.adhoc_plan_count >= 100 OR a.adhoc_plan_mb >= 512)
    """
)

# Database, storage, I/O, memory, CPU, and blocking.
add_candidate_sql(
    f"""
    SELECT 'DB001' AS rule_id, canonical_server_name, snapshot_date,
           'DATABASE' AS entity_type, database_name AS entity_name,
           'CRITICAL' AS severity, 100.0 AS priority_score,
           concat('Database ', database_name, ' is in state ', state_desc, '.') AS finding_summary,
           'The database is not available in the normal ONLINE state.' AS likely_cause,
           to_json(named_struct('database', database_name, 'state_desc', state_desc, 'user_access_desc', user_access_desc)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_database_health')}
    WHERE run_id = '{RUN_ID}' AND upper(coalesce(state_desc, 'UNKNOWN')) <> 'ONLINE'
    """
)

add_candidate_sql(
    f"""
    SELECT 'DB002' AS rule_id, canonical_server_name, snapshot_date,
           'DATABASE' AS entity_type, database_name AS entity_name,
           'HIGH' AS severity, 84.0 AS priority_score,
           concat('Database ', database_name, ' uses PAGE_VERIFY ', coalesce(page_verify_option, 'NULL'), ' instead of CHECKSUM.') AS finding_summary,
           'The database page-verification option is not set to the Microsoft best-practice value.' AS likely_cause,
           to_json(named_struct('database', database_name, 'page_verify_option', page_verify_option)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_database_health')}
    WHERE run_id = '{RUN_ID}' AND upper(coalesce(page_verify_option, '')) <> 'CHECKSUM'
    """
)

add_candidate_sql(
    f"""
    SELECT 'DB003' AS rule_id, canonical_server_name, snapshot_date,
           'DATABASE' AS entity_type, database_name AS entity_name,
           'HIGH' AS severity, 80.0 AS priority_score,
           concat('Database ', database_name, ' has ',
                  CASE WHEN is_auto_close_on THEN 'AUTO_CLOSE ' ELSE '' END,
                  CASE WHEN is_auto_shrink_on THEN 'AUTO_SHRINK ' ELSE '' END,
                  'enabled.') AS finding_summary,
           'One or more database options conflict with Microsoft Policy-Based Management best practices.' AS likely_cause,
           to_json(named_struct('database', database_name, 'is_auto_close_on', is_auto_close_on, 'is_auto_shrink_on', is_auto_shrink_on)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_database_health')}
    WHERE run_id = '{RUN_ID}' AND (is_auto_close_on = true OR is_auto_shrink_on = true)
    """
)

add_candidate_sql(
    f"""
    SELECT 'DB004' AS rule_id, canonical_server_name, snapshot_date,
           'DATABASE' AS entity_type, database_name AS entity_name,
           CASE WHEN log_used_percent >= 90 THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN log_used_percent >= 90 THEN 94.0 ELSE 78.0 END AS priority_score,
           concat('Database ', database_name, ' transaction log is ', cast(round(log_used_percent, 1) AS STRING), '% used; reuse wait is ', coalesce(log_reuse_wait_desc, 'UNKNOWN'), '.') AS finding_summary,
           'Transaction-log reuse is not keeping pace with current log consumption.' AS likely_cause,
           to_json(named_struct('database', database_name, 'log_used_percent', log_used_percent, 'log_reuse_wait_desc', log_reuse_wait_desc)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_database_health')}
    WHERE run_id = '{RUN_ID}' AND log_used_percent >= 80
    """
)

add_candidate_sql(
    f"""
    SELECT 'DB005' AS rule_id, canonical_server_name, snapshot_date,
           'DATABASE' AS entity_type, entity_name AS entity_name,
           CASE WHEN try_cast(row_values['VLF Count'] AS DOUBLE) > 1000 THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN try_cast(row_values['VLF Count'] AS DOUBLE) > 1000 THEN 92.0 ELSE 76.0 END AS priority_score,
           concat('Database ', entity_name, ' has ', row_values['VLF Count'], ' virtual log files.') AS finding_summary,
           'Transaction-log growth history created more VLFs than the Microsoft diagnostic example threshold.' AS likely_cause,
           to_json(named_struct('database', entity_name, 'vlf_count', try_cast(row_values['VLF Count'] AS DOUBLE))) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('agent_sql_rows_silver')}
    WHERE run_id = '{RUN_ID}' AND sheet_name = '35-VLF Counts'
      AND try_cast(row_values['VLF Count'] AS DOUBLE) > 100
    """
)

add_candidate_sql(
    f"""
    SELECT 'ST001' AS rule_id, canonical_server_name, snapshot_date,
           'STORAGE_VOLUME' AS entity_type, volume_path AS entity_name,
           CASE WHEN space_free_percent <= {DISK_CRITICAL_FREE_PERCENT} THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN space_free_percent <= {DISK_CRITICAL_FREE_PERCENT} THEN 96.0 ELSE 82.0 END AS priority_score,
           concat('Volume ', volume_path, ' has ', cast(round(space_free_percent, 1) AS STRING), '% free (', cast(round(available_size_gb, 1) AS STRING), ' GB).') AS finding_summary,
           'The volume is approaching capacity and may not support expected database, log, tempdb, or backup growth.' AS likely_cause,
           to_json(named_struct('volume_path', volume_path, 'total_size_gb', total_size_gb, 'available_size_gb', available_size_gb, 'space_free_percent', space_free_percent)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_storage_health')}
    WHERE run_id = '{RUN_ID}' AND space_free_percent <= {DISK_HIGH_FREE_PERCENT}
    """
)

add_candidate_sql(
    f"""
    SELECT 'IO001' AS rule_id, canonical_server_name, snapshot_date,
           io_scope AS entity_type, concat(coalesce(entity_name, 'UNKNOWN'), ' | ', coalesce(physical_path, '')) AS entity_name,
           CASE WHEN greatest(coalesce(read_latency_ms, 0), coalesce(write_latency_ms, 0), coalesce(overall_latency_ms, 0)) >= {IO_CRITICAL_LATENCY_MS} THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN greatest(coalesce(read_latency_ms, 0), coalesce(write_latency_ms, 0), coalesce(overall_latency_ms, 0)) >= {IO_CRITICAL_LATENCY_MS} THEN 96.0 ELSE 82.0 END AS priority_score,
           concat(io_scope, ' ', coalesce(entity_name, physical_path), ' latency: read=', cast(round(read_latency_ms, 1) AS STRING), 'ms, write=', cast(round(write_latency_ms, 1) AS STRING), 'ms, overall=', cast(round(overall_latency_ms, 1) AS STRING), 'ms.') AS finding_summary,
           'The observed storage response time exceeds the configured operational threshold.' AS likely_cause,
           to_json(named_struct('io_scope', io_scope, 'entity_name', entity_name, 'physical_path', physical_path, 'read_latency_ms', read_latency_ms, 'write_latency_ms', write_latency_ms, 'overall_latency_ms', overall_latency_ms)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_io_health')}
    WHERE run_id = '{RUN_ID}'
      AND greatest(coalesce(read_latency_ms, 0), coalesce(write_latency_ms, 0), coalesce(overall_latency_ms, 0)) >= {IO_HIGH_LATENCY_MS}
    """
)

add_candidate_sql(
    f"""
    SELECT 'MEM001' AS rule_id, canonical_server_name, snapshot_date,
           'SERVER_MEMORY' AS entity_type, canonical_server_name AS entity_name,
           CASE WHEN memory_grants_pending >= 5 THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN memory_grants_pending >= 5 THEN 94.0 ELSE 82.0 END AS priority_score,
           concat(cast(memory_grants_pending AS STRING), ' queries are waiting for memory grants.') AS finding_summary,
           'Workspace memory is not immediately available for one or more query operators.' AS likely_cause,
           to_json(named_struct('memory_grants_pending', memory_grants_pending)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_memory_health')}
    WHERE run_id = '{RUN_ID}' AND sheet_name = '48-Memory Grants Pending' AND memory_grants_pending > 0
    """
)

add_candidate_sql(
    f"""
    SELECT 'MEM002' AS rule_id, canonical_server_name, snapshot_date,
           'SERVER_MEMORY' AS entity_type, canonical_server_name AS entity_name,
           'CRITICAL' AS severity, 100.0 AS priority_score,
           'SQL Server process-memory pressure flags are active.' AS finding_summary,
           'The SQL Server process reports low physical or virtual memory.' AS likely_cause,
           to_json(named_struct('process_physical_memory_low', row_values['process_physical_memory_low'], 'process_virtual_memory_low', row_values['process_virtual_memory_low'], 'memory_utilization_percentage', row_values['memory_utilization_percentage'])) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('agent_sql_rows_silver')}
    WHERE run_id = '{RUN_ID}' AND sheet_name = '6-Process Memory'
      AND (lower(row_values['process_physical_memory_low']) = 'true' OR lower(row_values['process_virtual_memory_low']) = 'true')
    """
)

add_candidate_sql(
    f"""
    SELECT 'MEM003' AS rule_id, canonical_server_name, snapshot_date,
           'SERVER_MEMORY' AS entity_type, canonical_server_name AS entity_name,
           CASE WHEN available_memory_mb / physical_memory_mb * 100 < 5 THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN available_memory_mb / physical_memory_mb * 100 < 5 THEN 94.0 ELSE 80.0 END AS priority_score,
           concat('Available physical memory is ', cast(round(available_memory_mb / physical_memory_mb * 100, 1) AS STRING), '% (', cast(round(available_memory_mb, 0) AS STRING), ' MB).') AS finding_summary,
           'Host memory availability is below the operational guardrail.' AS likely_cause,
           to_json(named_struct('physical_memory_mb', physical_memory_mb, 'available_memory_mb', available_memory_mb, 'available_percent', available_memory_mb / physical_memory_mb * 100, 'system_memory_state', system_memory_state)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_memory_health')}
    WHERE run_id = '{RUN_ID}' AND sheet_name = '14-System Memory'
      AND physical_memory_mb > 0 AND available_memory_mb / physical_memory_mb * 100 < 10
    """
)

add_candidate_sql(
    f"""
    WITH cpu AS (
        SELECT canonical_server_name, snapshot_date,
               AVG(sql_cpu_percent) AS average_sql_cpu_percent,
               MAX(sql_cpu_percent) AS peak_sql_cpu_percent,
               COUNT(*) AS sample_count,
               MAX(event_time) AS latest_event_time
        FROM {table_name('v_agent_cpu_health')}
        WHERE run_id = '{RUN_ID}' AND sheet_name = '45-CPU Utilization History'
        GROUP BY canonical_server_name, snapshot_date
    )
    SELECT 'CPU001' AS rule_id, canonical_server_name, snapshot_date,
           'SERVER_CPU' AS entity_type, canonical_server_name AS entity_name,
           CASE WHEN average_sql_cpu_percent >= {CPU_CRITICAL_PERCENT} THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN average_sql_cpu_percent >= {CPU_CRITICAL_PERCENT} THEN 94.0 ELSE 80.0 END AS priority_score,
           concat('Average SQL Server CPU is ', cast(round(average_sql_cpu_percent, 1) AS STRING), '% across ', cast(sample_count AS STRING), ' samples; peak=', cast(round(peak_sql_cpu_percent, 1) AS STRING), '%.') AS finding_summary,
           'SQL Server CPU utilization is consistently above the configured operational threshold.' AS likely_cause,
           to_json(named_struct('average_sql_cpu_percent', average_sql_cpu_percent, 'peak_sql_cpu_percent', peak_sql_cpu_percent, 'sample_count', sample_count)) AS evidence_json,
           latest_event_time AS source_observed_ts
    FROM cpu
    WHERE average_sql_cpu_percent >= {CPU_HIGH_PERCENT}
    """
)

add_candidate_sql(
    f"""
    SELECT 'BLK001' AS rule_id, canonical_server_name, snapshot_date,
           'BLOCKING_SESSION' AS entity_type, concat(coalesce(database_name, 'UNKNOWN'), ':', cast(blocker_session_id AS STRING), '->', cast(waiter_session_id AS STRING)) AS entity_name,
           CASE WHEN wait_time_ms >= {BLOCKING_CRITICAL_SECONDS * 1000} THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
           CASE WHEN wait_time_ms >= {BLOCKING_CRITICAL_SECONDS * 1000} THEN 98.0 ELSE 84.0 END AS priority_score,
           concat('Session ', cast(waiter_session_id AS STRING), ' waited ', cast(round(wait_time_ms / 1000.0, 1) AS STRING), ' seconds for blocker ', cast(blocker_session_id AS STRING), ' in ', coalesce(database_name, 'UNKNOWN'), '.') AS finding_summary,
           'A blocker held an incompatible lock beyond the configured operational duration.' AS likely_cause,
           to_json(named_struct('database', database_name, 'lock_type', lock_type, 'lock_request', lock_request, 'waiter_session_id', waiter_session_id, 'blocker_session_id', blocker_session_id, 'wait_time_ms', wait_time_ms, 'waiter_statement', waiter_statement, 'blocker_batch', blocker_batch)) AS evidence_json,
           cast(snapshot_date AS TIMESTAMP) AS source_observed_ts
    FROM {table_name('v_agent_blocking_health')}
    WHERE run_id = '{RUN_ID}' AND wait_time_ms >= {BLOCKING_HIGH_SECONDS * 1000}
    """
)

# Windows event signatures. Test runs are evaluated at the source's own latest
# date, while production freshness remains visible through DQ003.
windows_rules = [
    ("BK005", "(event_id = '3041' OR lower(sample_message) RLIKE 'backup.*failed to complete|backup.*failure')", "WINDOWS_EVENT", "CRITICAL", 100.0, "Recent events report a backup failure."),
    ("WN001", "((lower(provider_name) RLIKE 'disk|ntfs|storport|stornvme|iastor|volmgr') AND event_id IN ('7','9','11','15','51','55','129','140','153','157'))", "WINDOWS_STORAGE_EVENT", "CRITICAL", 100.0, "Windows reports a storage timeout, reset, retry, corruption, or device-loss event."),
    ("WN002", "((lower(provider_name) RLIKE 'kernel-power|eventlog') AND event_id IN ('41','6008'))", "WINDOWS_EVENT", "CRITICAL", 100.0, "Windows recorded an unexpected shutdown or restart."),
    ("WN003", "((lower(provider_name) RLIKE 'defender|malware') AND event_id IN ('1116','1119','5010'))", "WINDOWS_SECURITY_EVENT", "CRITICAL", 100.0, "Microsoft Defender reported malware, failed remediation, or disabled protection."),
    ("WN004", "(event_id = '4740' OR (event_id = '4625' AND source_occurrences >= 5))", "WINDOWS_SECURITY_EVENT", "HIGH", 88.0, "Windows recorded account lockout or repeated failed-logon activity."),
    ("WN005", "(lower(provider_name) RLIKE 'service control manager' AND event_id IN ('7031','7034'))", "WINDOWS_SERVICE_EVENT", "HIGH", 84.0, "A Windows service terminated unexpectedly."),
    ("WN006", "(lower(provider_name) RLIKE 'security-kerberos' AND event_id = '4')", "WINDOWS_SECURITY_EVENT", "HIGH", 88.0, "Kerberos reported KRB_AP_ERR_MODIFIED, consistent with an SPN or service-account mismatch."),
    ("WN007", "(lower(provider_name) = 'lsasrv' AND event_id = '6038')", "WINDOWS_SECURITY_EVENT", "MEDIUM", 58.0, "Windows detected NTLM authentication use."),
    ("WN008", "(lower(provider_name) = 'volsnap' AND event_id = '36')", "WINDOWS_BACKUP_EVENT", "HIGH", 90.0, "A VSS shadow copy was aborted because storage could not grow."),
    ("WN009", "(lower(provider_name) RLIKE 'grouppolicy' AND event_id IN ('1030','1054','1085') AND source_occurrences >= 3)", "WINDOWS_POLICY_EVENT", "HIGH", 82.0, "Repeated Group Policy processing failures were recorded."),
    ("WN010", "(lower(provider_name) = 'schannel' AND event_id IN ('36874','36888') AND source_occurrences >= 5)", "WINDOWS_SECURITY_EVENT", "HIGH", 82.0, "Repeated Schannel TLS handshake failures were recorded."),
]

for rule_id, condition, entity_type, severity, score, summary in windows_rules:
    add_candidate_sql(
        f"""
        SELECT '{rule_id}' AS rule_id, canonical_server_name, snapshot_date,
               '{entity_type}' AS entity_type,
               concat(coalesce(provider_name, 'UNKNOWN'), ':', coalesce(event_id, 'UNKNOWN')) AS entity_name,
               '{severity}' AS severity, {score} AS priority_score,
               '{summary}' AS finding_summary,
               substr(sample_message, 1, 4000) AS likely_cause,
               to_json(named_struct('provider_name', provider_name, 'event_id', event_id, 'severity_class', severity_class, 'source_occurrences', source_occurrences, 'unique_events', unique_events, 'latest_event_time', latest_event_time, 'sample_message', substr(sample_message, 1, 4000))) AS evidence_json,
               latest_event_time AS source_observed_ts
        FROM _agent_windows_recent_grouped
        WHERE may_evaluate_windows_rules = true AND {condition}
        """
    )

# Parse every rule query independently before unioning the resulting
# DataFrames. Several rule queries legitimately start with a WITH clause. A
# single SQL string assembled as ``query UNION ALL WITH ...`` is invalid Spark
# SQL because a CTE cannot begin a later UNION branch.
candidate_frames = []
for block_number, candidate_sql in enumerate(candidate_sql_blocks, start=1):
    try:
        candidate_frames.append(spark.sql(candidate_sql))
    except Exception as exc:
        raise RuntimeError(
            f"Health-rule SQL block {block_number} could not be parsed or analyzed."
        ) from exc

if not candidate_frames:
    raise RuntimeError("No enabled health-rule SQL blocks were configured.")

candidate_df = candidate_frames[0]
for candidate_frame in candidate_frames[1:]:
    candidate_df = candidate_df.unionByName(candidate_frame)

# Keep one deterministic row per server/rule/entity. This removes workbook or
# event duplicates without hiding separate affected databases, jobs, or files.
candidate_dedupe_window = Window.partitionBy(
    "canonical_server_name",
    "rule_id",
    F.coalesce(F.col("entity_name"), F.lit("")),
).orderBy(
    F.col("priority_score").desc(),
    F.col("source_observed_ts").desc_nulls_last(),
)

candidate_df = (
    candidate_df
    .withColumn("_candidate_rank", F.row_number().over(candidate_dedupe_window))
    .where(F.col("_candidate_rank") == 1)
    .drop("_candidate_rank")
)


# -----------------------------------------------------------------------------
# 6. Enrich, preserve issue history, and idempotently merge findings
# -----------------------------------------------------------------------------

active_rules_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_rule_catalog")
    .where(F.col("is_enabled") == True)
    .select(
        "rule_id",
        "rule_version",
        "domain",
        "rule_title",
        "recommended_action",
        "microsoft_reference_url",
        "threshold_note",
    )
)

candidate_df = (
    candidate_df
    .join(F.broadcast(active_rules_df), "rule_id", "inner")
    .withColumn(
        "issue_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("canonical_server_name"),
                F.col("rule_id"),
                F.coalesce(F.col("entity_type"), F.lit("")),
                F.coalesce(F.col("entity_name"), F.lit("")),
            ),
            256,
        ),
    )
    .withColumn(
        "finding_id",
        F.sha2(F.concat_ws("||", F.lit(RUN_ID), F.col("issue_key")), 256),
    )
)

prior_history_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_findings")
    .where(F.col("run_id") != RUN_ID)
    .groupBy("issue_key")
    .agg(
        F.min("first_detected_ts").alias("historical_first_detected_ts"),
        F.countDistinct("run_id").cast("long").alias("historical_run_count"),
    )
)

finding_context = "HISTORICAL_SAMPLE_TEST" if IS_TEST_RUN else "PRODUCTION_CURRENT_RUN"

findings_df = (
    candidate_df
    .join(prior_history_df, "issue_key", "left")
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("finding_context", F.lit(finding_context))
    .withColumn("finding_status", F.lit("OPEN"))
    .withColumn(
        "first_detected_ts",
        F.coalesce(
            F.col("historical_first_detected_ts"),
            F.lit(NOW_UTC).cast("timestamp"),
        ),
    )
    .withColumn("last_detected_ts", F.lit(NOW_UTC).cast("timestamp"))
    .withColumn(
        "detected_run_count",
        (F.coalesce(F.col("historical_run_count"), F.lit(0)) + F.lit(1)).cast("long"),
    )
    .withColumn("created_ts", F.lit(NOW_UTC).cast("timestamp"))
    .withColumn("updated_ts", F.lit(NOW_UTC).cast("timestamp"))
    .select(
        "finding_id",
        "issue_key",
        "run_id",
        "snapshot_date",
        "canonical_server_name",
        "rule_id",
        "rule_version",
        "domain",
        "severity",
        "priority_score",
        "rule_title",
        "entity_type",
        "entity_name",
        "finding_summary",
        "likely_cause",
        "evidence_json",
        "recommended_action",
        "microsoft_reference_url",
        "threshold_note",
        "source_observed_ts",
        "finding_context",
        "finding_status",
        "first_detected_ts",
        "last_detected_ts",
        "detected_run_count",
        "created_ts",
        "updated_ts",
    )
)

findings_df.createOrReplaceTempView("_agent_findings_source")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_findings')} AS target
    USING _agent_findings_source AS source
       ON target.finding_id = source.finding_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND target.run_id = '{RUN_ID}' THEN DELETE
    """
)

spark.catalog.dropTempView("_agent_findings_source")


# -----------------------------------------------------------------------------
# 7. Rank top issues and build one health summary per server
# -----------------------------------------------------------------------------

severity_order = (
    F.when(F.col("severity") == "CRITICAL", F.lit(4))
    .when(F.col("severity") == "HIGH", F.lit(3))
    .when(F.col("severity") == "MEDIUM", F.lit(2))
    .when(F.col("severity") == "LOW", F.lit(1))
    .otherwise(F.lit(0))
)

rank_window = Window.partitionBy("canonical_server_name").orderBy(
    severity_order.desc(),
    F.col("priority_score").desc(),
    F.col("rule_id"),
    F.col("entity_name"),
)

ranked_findings_df = findings_df.withColumn(
    "finding_rank",
    F.row_number().over(rank_window),
)

finding_counts_df = (
    findings_df
    .groupBy("canonical_server_name")
    .agg(
        F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).cast("int").alias("critical_issue_count"),
        F.sum(F.when(F.col("severity") == "HIGH", 1).otherwise(0)).cast("int").alias("high_issue_count"),
        F.sum(F.when(F.col("severity") == "MEDIUM", 1).otherwise(0)).cast("int").alias("medium_issue_count"),
        F.sum(F.when(F.col("severity") == "LOW", 1).otherwise(0)).cast("int").alias("low_issue_count"),
        F.sum(F.when((F.col("domain") == "DATA_QUALITY") & (F.col("severity") == "CRITICAL"), 1).otherwise(0)).cast("int").alias("data_quality_blocker_count"),
        F.count("*").cast("int").alias("total_actionable_issue_count"),
    )
)

top_findings_df = (
    ranked_findings_df
    .where(F.col("finding_rank") <= TOP_ISSUES_PER_SERVER)
    .groupBy("canonical_server_name")
    .agg(
        F.sort_array(
            F.collect_list(F.struct("finding_rank", "finding_id"))
        ).alias("_ranked_ids")
    )
    .withColumn(
        "top_finding_ids",
        F.expr("transform(_ranked_ids, item -> item.finding_id)"),
    )
    .drop("_ranked_ids")
)

zero_counts = {
    "critical_issue_count": 0,
    "high_issue_count": 0,
    "medium_issue_count": 0,
    "low_issue_count": 0,
    "data_quality_blocker_count": 0,
    "total_actionable_issue_count": 0,
}

health_summary_df = (
    inventory_df
    .select(
        "run_id",
        "snapshot_date",
        "canonical_server_name",
        "latest_observed_ts",
    )
    .join(finding_counts_df, "canonical_server_name", "left")
    .join(top_findings_df, "canonical_server_name", "left")
    .fillna(zero_counts)
    .withColumn(
        "top_finding_ids",
        F.coalesce(F.col("top_finding_ids"), F.expr("array()").cast("array<string>")),
    )
    .withColumn(
        "health_status",
        F.when(F.col("data_quality_blocker_count") > 0, F.lit("DATA_INCOMPLETE"))
        .when(F.col("critical_issue_count") > 0, F.lit("CRITICAL"))
        .when(F.col("high_issue_count") > 0, F.lit("ATTENTION"))
        .when(F.col("medium_issue_count") > 0, F.lit("WATCH"))
        .otherwise(F.lit("HEALTHY")),
    )
    .withColumn(
        "health_score",
        F.when(F.col("data_quality_blocker_count") > 0, F.lit(None).cast("double"))
        .otherwise(
            F.greatest(
                F.lit(0.0),
                F.lit(100.0)
                - F.col("critical_issue_count") * F.lit(30.0)
                - F.col("high_issue_count") * F.lit(12.0)
                - F.col("medium_issue_count") * F.lit(5.0)
                - F.col("low_issue_count") * F.lit(2.0),
            )
        ),
    )
    .withColumn("summary_context", F.lit(finding_context))
    .withColumn("evaluated_ts", F.lit(NOW_UTC).cast("timestamp"))
    .select(
        "run_id",
        "snapshot_date",
        "canonical_server_name",
        "health_status",
        "health_score",
        "critical_issue_count",
        "high_issue_count",
        "medium_issue_count",
        "low_issue_count",
        "data_quality_blocker_count",
        "total_actionable_issue_count",
        "top_finding_ids",
        "latest_observed_ts",
        "summary_context",
        "evaluated_ts",
    )
)

health_summary_df.createOrReplaceTempView("_agent_server_health_summary_source")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_server_health_summary')} AS target
    USING _agent_server_health_summary_source AS source
       ON target.run_id = source.run_id
      AND target.canonical_server_name = source.canonical_server_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND target.run_id = '{RUN_ID}' THEN DELETE
    """
)

spark.catalog.dropTempView("_agent_server_health_summary_source")


# -----------------------------------------------------------------------------
# 8. Consumption views and orchestration status
# -----------------------------------------------------------------------------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_current_findings')} AS
    SELECT finding.*
    FROM {table_name('agent_findings')} AS finding
    INNER JOIN (
        SELECT canonical_server_name, MAX(snapshot_date) AS latest_snapshot_date
        FROM {table_name('agent_findings')}
        GROUP BY canonical_server_name
    ) AS latest
       ON finding.canonical_server_name = latest.canonical_server_name
      AND finding.snapshot_date = latest.latest_snapshot_date
    WHERE finding.finding_status = 'OPEN'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_top_server_findings')} AS
    SELECT finding.*, ranked.finding_rank
    FROM {table_name('v_agent_current_findings')} AS finding
    INNER JOIN (
        SELECT
            finding_id,
            ROW_NUMBER() OVER (
                PARTITION BY canonical_server_name
                ORDER BY
                    CASE severity
                        WHEN 'CRITICAL' THEN 4
                        WHEN 'HIGH' THEN 3
                        WHEN 'MEDIUM' THEN 2
                        WHEN 'LOW' THEN 1
                        ELSE 0
                    END DESC,
                    priority_score DESC,
                    rule_id,
                    entity_name
            ) AS finding_rank
        FROM {table_name('v_agent_current_findings')}
    ) AS ranked
      ON finding.finding_id = ranked.finding_id
    WHERE ranked.finding_rank <= {TOP_ISSUES_PER_SERVER}
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_latest_server_health_summary')} AS
    SELECT summary.*
    FROM {table_name('agent_server_health_summary')} AS summary
    INNER JOIN (
        SELECT canonical_server_name, MAX(snapshot_date) AS latest_snapshot_date
        FROM {table_name('agent_server_health_summary')}
        GROUP BY canonical_server_name
    ) AS latest
       ON summary.canonical_server_name = latest.canonical_server_name
      AND summary.snapshot_date = latest.latest_snapshot_date
    """
)

FINAL_RUN_STATUS = (
    "TEST_HEALTH_RULES_EVALUATED" if IS_TEST_RUN else "HEALTH_RULES_EVALUATED"
)
FINAL_ERROR_MESSAGE = (
    "Health rules were evaluated against historical sample data under "
    "allow_incomplete_run=true."
    if IS_TEST_RUN
    else None
)

run_update_df = spark.createDataFrame(
    [(RUN_ID, FINAL_RUN_STATUS, NOW_UTC, FINAL_ERROR_MESSAGE, NOW_UTC)],
    "run_id STRING, run_status STRING, processing_completed_ts TIMESTAMP, "
    "error_message STRING, updated_ts TIMESTAMP",
)
run_update_df.createOrReplaceTempView("_agent_rules_run_update")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_ingestion_runs')} AS target
    USING _agent_rules_run_update AS source
       ON target.run_id = source.run_id
    WHEN MATCHED THEN UPDATE SET
        target.run_status = source.run_status,
        target.processing_completed_ts = source.processing_completed_ts,
        target.error_message = source.error_message,
        target.updated_ts = source.updated_ts
    """
)

spark.catalog.dropTempView("_agent_rules_run_update")

finding_count = findings_df.count()
server_count = health_summary_df.count()

summary_rows = [
    ("run_id", RUN_ID),
    ("rule_version", RULE_VERSION),
    ("enabled_rule_count", str(rule_catalog_df.count())),
    ("finding_count", str(finding_count)),
    ("server_health_summary_count", str(server_count)),
    ("top_issues_per_server", str(TOP_ISSUES_PER_SERVER)),
    ("final_run_status", FINAL_RUN_STATUS),
]

display(spark.createDataFrame(summary_rows, ["check", "value"]))

display(
    findings_df
    .groupBy("severity", "domain")
    .agg(F.count("*").alias("finding_count"))
    .orderBy(
        F.when(F.col("severity") == "CRITICAL", 1)
        .when(F.col("severity") == "HIGH", 2)
        .when(F.col("severity") == "MEDIUM", 3)
        .otherwise(4),
        "domain",
    )
)

display(
    health_summary_df
    .orderBy(
        F.when(F.col("health_status") == "DATA_INCOMPLETE", 1)
        .when(F.col("health_status") == "CRITICAL", 2)
        .when(F.col("health_status") == "ATTENTION", 3)
        .when(F.col("health_status") == "WATCH", 4)
        .otherwise(5),
        "canonical_server_name",
    )
)

print(f"Final run status: {FINAL_RUN_STATUS}")

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    dbutils.jobs.taskValues.set(key="health_rules_status", value=FINAL_RUN_STATUS)
except Exception:
    pass