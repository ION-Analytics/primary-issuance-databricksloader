# Primary Issuance: Daily Incremental Run

Use this runbook for regular daily loading after successful full initial setup.

## Daily sequence

```text
1. Start AWS proxy
2. Run Loan incremental locally
3. Validate Loan raw table
4. Run Bond incremental locally
5. Validate Bond raw table
6. Run parsed incremental MERGE in Databricks
7. Refresh operational search projection in Databricks
8. Run validation SQL
```

Do not run initial load during daily operation.

---

# Part 1: Common local environment

Open PowerShell in loader folder.

```powershell
$Env:DATABRICKS_SERVER_HOSTNAME="adb-1883700548140357.17.azuredatabricks.net"
$Env:DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/05e0deb0358ff310"
$Env:DATABRICKS_TOKEN="<token>"

$Env:ES_PROXY_URL="http://localhost:8090"
$Env:ES_INDEX="deal_documents_latest"

$Env:VOLUME_PATH="/Volumes/productivity_dev/primaryissuance/primaryissuance_elasticsearch"
$Env:ES_BATCH_SIZE="5000"
$Env:MAX_PAGES="0"
```

---

# Part 2: Start AWS signing proxy

Set temporary AWS credentials: https://d-936704671c.awsapps.com/start/#/?tab=accounts

```powershell
$Env:AWS_ACCESS_KEY_ID="<access-key>"
$Env:AWS_SECRET_ACCESS_KEY="<secret-key>"
$Env:AWS_SESSION_TOKEN="<session-token>"
```

Start proxy in separate terminal:

```powershell
Remove-Item -Path Alias:curl -Force -ErrorAction SilentlyContinue

docker run --rm `
  -e AWS_ACCESS_KEY_ID `
  -e AWS_SECRET_ACCESS_KEY `
  -e AWS_SESSION_TOKEN `
  -p 8090:8090 `
  cllunsford/aws-signing-proxy:latest `
  -region eu-west-1 `
  -target https://vpc-live-pisearchindexer-g5mrjw3cuxijfpba6kwqgq5w2m.eu-west-1.es.amazonaws.com/ `
  -port 8090
```

---

# Part 3: Run Loan incremental locally

## 3.1 Set complete Loan environment

```powershell
$Env:INSTRUMENT_TYPE="Loan"
$Env:RAW_TABLE="loans_raw"
$Env:CHECKPOINT_TABLE="loans_ingestion_checkpoint"
$Env:RUN_TABLE="loans_ingestion_runs"
$Env:LOCAL_STAGING_DIR="$PWD\primary_issuance_v5_staging\loan"
```

## 3.2 Verify Loan configuration

```powershell
if ($Env:INSTRUMENT_TYPE -ne "Loan") { throw "Expected Loan" }
if ($Env:RAW_TABLE -ne "loans_raw") { throw "Expected loans_raw" }
if ($Env:CHECKPOINT_TABLE -ne "loans_ingestion_checkpoint") { throw "Expected Loan checkpoint" }
if ($Env:RUN_TABLE -ne "loans_ingestion_runs") { throw "Expected Loan run table" }

Write-Host "Loan configuration valid"
```

## 3.3 Run incremental extraction

```powershell
python primary_issuance_ingest_v5.py incremental
```

Expected log:

```text
Incremental strictly after <timestamp>; no overlap and no historical scan
```

Never run this during daily cycle:

```powershell
python primary_issuance_ingest_v5.py initial --reset
```

## 3.4 Validate Loan raw table in Databricks

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT document_id) AS documents,
    COUNT(*) - COUNT(DISTINCT document_id) AS duplicates
FROM productivity_dev.primaryissuance.loans_raw;
```

Expected:

```text
duplicates = 0
```

```sql
SELECT COUNT(*) AS invalid_domain_rows
FROM productivity_dev.primaryissuance.loans_raw
WHERE instrument_type <> 'Loan'
   OR get_json_object(source_json, '$.instrumentType') <> 'Loan'
   OR instrument_type IS NULL;
```

Expected:

```text
invalid_domain_rows = 0
```

## 3.5 Check Loan run status

```sql
SELECT
    run_id,
    mode,
    status,
    started_at,
    finished_at,
    documents_read,
    documents_written,
    error_message,
    pipeline_version
FROM productivity_dev.primaryissuance.loans_ingestion_runs
ORDER BY started_at DESC
LIMIT 5;
```

Latest run should be `SUCCESS`.

---

# Part 4: Run Bond incremental locally

## 4.1 Set complete Bond environment

```powershell
$Env:INSTRUMENT_TYPE="Bond"
$Env:RAW_TABLE="bonds_raw"
$Env:CHECKPOINT_TABLE="bonds_ingestion_checkpoint"
$Env:RUN_TABLE="bonds_ingestion_runs"
$Env:LOCAL_STAGING_DIR="$PWD\primary_issuance_v5_staging\bond"
```

## 4.2 Verify Bond configuration

```powershell
if ($Env:INSTRUMENT_TYPE -ne "Bond") { throw "Expected Bond" }
if ($Env:RAW_TABLE -ne "bonds_raw") { throw "Expected bonds_raw" }
if ($Env:CHECKPOINT_TABLE -ne "bonds_ingestion_checkpoint") { throw "Expected Bond checkpoint" }
if ($Env:RUN_TABLE -ne "bonds_ingestion_runs") { throw "Expected Bond run table" }

Write-Host "Bond configuration valid"
```

## 4.3 Run incremental extraction

```powershell
python primary_issuance_ingest_v5.py incremental
```

Expected log:

```text
Incremental strictly after <timestamp>; no overlap and no historical scan
```

## 4.4 Validate Bond raw table in Databricks

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT document_id) AS documents,
    COUNT(*) - COUNT(DISTINCT document_id) AS duplicates
FROM productivity_dev.primaryissuance.bonds_raw;
```

Expected:

```text
duplicates = 0
```

```sql
SELECT COUNT(*) AS invalid_domain_rows
FROM productivity_dev.primaryissuance.bonds_raw
WHERE instrument_type <> 'Bond'
   OR get_json_object(source_json, '$.instrumentType') <> 'Bond'
   OR instrument_type IS NULL;
```

Expected:

```text
invalid_domain_rows = 0
```

## 4.5 Check Bond run status

```sql
SELECT
    run_id,
    mode,
    status,
    started_at,
    finished_at,
    documents_read,
    documents_written,
    error_message,
    pipeline_version
FROM productivity_dev.primaryissuance.bonds_ingestion_runs
ORDER BY started_at DESC
LIMIT 5;
```

Latest run should be `SUCCESS`.

---

# Part 5: Run parsed incremental MERGE in Databricks

Only run after both Loan and Bond ingestion commands complete successfully.

Notebook name : Primary Issuance Data Ingestion

URL : https://adb-1883700548140357.17.azuredatabricks.net/editor/notebooks/4128949757694498?o=1883700548140357 

Run entire file in one Databricks SQL session:

```text
03_parsed_incremental_merge.sql
```

This script:

- Reads transformation watermark
- Finds raw documents changed since watermark
- Parses affected deals only
- Inserts new parsed records
- Updates changed parsed records
- Removes deleted child relationships from affected deals
- Advances watermark after successful completion

Parsed tables are current-state tables. They use `MERGE`, not blind append.

---

# Part 6: Refresh search projection in Databricks

Run:

```text
06_operational_search_projection.sql
```

Search projection remains in:

```text
productivity_dev.primaryissuance.deal_search_projection
```

It is not stored in parsed consumer schema.

---

# Part 7: Run validation in Databricks

Run:

```text
07_validation.sql
```

## Daily minimum validation

### Raw uniqueness

```sql
SELECT 'Loan' AS domain,
       COUNT(*) AS rows,
       COUNT(DISTINCT document_id) AS documents,
       COUNT(*) - COUNT(DISTINCT document_id) AS duplicates
FROM productivity_dev.primaryissuance.loans_raw
UNION ALL
SELECT 'Bond',
       COUNT(*),
       COUNT(DISTINCT document_id),
       COUNT(*) - COUNT(DISTINCT document_id)
FROM productivity_dev.primaryissuance.bonds_raw;
```

Expected:

```text
duplicates = 0 for both domains
```

### Parsed uniqueness

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT deal_key) AS keys,
    COUNT(*) - COUNT(DISTINCT deal_key) AS duplicates
FROM productivity_dev.primaryissuance_parsed.deals;
```

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT tranche_key) AS keys,
    COUNT(*) - COUNT(DISTINCT tranche_key) AS duplicates
FROM productivity_dev.primaryissuance_parsed.tranches;
```

Expected:

```text
duplicates = 0
```

### Landing tables

```sql
SELECT 'Loan' AS domain, COUNT(*) AS landing_rows
FROM productivity_dev.primaryissuance.loans_raw_landing
UNION ALL
SELECT 'Bond', COUNT(*)
FROM productivity_dev.primaryissuance.bonds_raw_landing;
```

Expected:

```text
landing_rows = 0 after successful runs
```

### Parsed watermark

```sql
SELECT *
FROM productivity_dev.primaryissuance.pipeline_watermarks
WHERE pipeline_name = 'primary_issuance_relational';
```

`last_processed_at` should match current daily cycle.

---

# Part 8: Failure handling

## Python run fails before raw MERGE

- Raw table unchanged
- Checkpoint unchanged
- Fix issue
- Rerun same domain incremental

## Python run fails after raw MERGE but before checkpoint save

- Some current raw rows may already be updated
- Rerun same incremental boundary
- Delta `MERGE` prevents duplicate raw rows

## Parsed incremental fails

- Do not manually advance transformation watermark
- Fix issue
- Rerun full `03_parsed_incremental_merge.sql` in one session

## Wrong domain environment detected

V5.1 should fail immediately if mappings do not match:

```text
Loan -> loans_raw / loans_ingestion_checkpoint / loans_ingestion_runs
Bond -> bonds_raw / bonds_ingestion_checkpoint / bonds_ingestion_runs
```

Do not bypass this validation.

## Incremental refuses because raw table has no dated data

This means domain has no usable starting timestamp.

Run initial load for that domain:

```powershell
python primary_issuance_ingest_v5.py initial --reset
```

Then future runs use incremental.

---

# Part 9: Important limitation

Documents without `lastModifiedDate` can be loaded during initial scan. Timestamp-based incremental cannot detect later changes to records that continue to have no timestamp.

Required long-term action:

- Fix missing timestamp upstream, or
- Run periodic reconciliation for missing-date documents

Do not silently assume those records are covered by daily incremental.

---

# Part 10: Daily command summary

## Local PowerShell

```text
Set Loan variables
python primary_issuance_ingest_v5.py incremental

Set Bond variables
python primary_issuance_ingest_v5.py incremental
```

## Databricks SQL

```text
03_parsed_incremental_merge.sql
06_operational_search_projection.sql
07_validation.sql
```
