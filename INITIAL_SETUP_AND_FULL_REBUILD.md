# Primary Issuance: Initial Setup and Full Rebuild

Use this runbook when:

- Setting up pipeline for first time
- Deleting all raw and parsed data and starting again
- Recovering from major cross-domain contamination
- Rebuilding after incompatible schema or pipeline changes

## Architecture

### Local machine

Python performs extraction and raw loading:

```text
Elasticsearch
-> compressed JSONL
-> Unity Catalog Volume
-> landing Delta table
-> MERGE into current raw table
```

### Databricks

Databricks SQL performs parsing and relational modeling:

```text
Raw Loan/Bond tables
-> operational source views
-> six parsed consumer tables
-> search projection
```

## Schemas

### Raw and operational schema

```text
productivity_dev.primaryissuance
```

Contains:

- `loans_raw`
- `bonds_raw`
- `loans_raw_landing`
- `bonds_raw_landing`
- ingestion checkpoints
- ingestion run tables
- transformation views
- transformation watermark
- search projection

### Parsed schema

```text
productivity_dev.primaryissuance_parsed
```

Contains only:

- `deals`
- `tranches`
- `companies`
- `deal_participants`
- `tranche_participants`
- `tranche_participant_roles`

---

# Part 1: Files required locally

Place these files in your loader folder:

```text
primary_issuance_ingest_v5.py
primary_issuance_databricks_clean_layout/
```

---

# Part 2: Stop existing processes

Stop:

- V3 loader
- V4 loader
- V5 loader
- Scheduled Loan job
- Scheduled Bond job
- Parsed incremental job

Do not run ingestion while tables are being dropped or rebuilt.

---

# Part 3: Common local environment

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

Install dependencies:

```powershell
pip install requests databricks-sql-connector
```

---

# Part 4: Start AWS signing proxy

Set temporary AWS credentials: https://d-936704671c.awsapps.com/start/#/?tab=accounts
```powershell
$Env:AWS_ACCESS_KEY_ID="<access-key>"
$Env:AWS_SECRET_ACCESS_KEY="<secret-key>"
$Env:AWS_SESSION_TOKEN="<session-token>"
```

Start proxy in a separate PowerShell terminal:

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

Keep proxy terminal running during extraction.

---

# Part 5: If you need to delete all pipeline data in Databricks

Run in Databricks SQL only if full reset is intended.

## 5.1 Drop parsed tables

```sql
DROP TABLE IF EXISTS productivity_dev.primaryissuance_parsed.deals;
DROP TABLE IF EXISTS productivity_dev.primaryissuance_parsed.tranches;
DROP TABLE IF EXISTS productivity_dev.primaryissuance_parsed.companies;
DROP TABLE IF EXISTS productivity_dev.primaryissuance_parsed.deal_participants;
DROP TABLE IF EXISTS productivity_dev.primaryissuance_parsed.tranche_participants;
DROP TABLE IF EXISTS productivity_dev.primaryissuance_parsed.tranche_participant_roles;
```

## 5.2 Drop raw and landing tables

```sql
DROP TABLE IF EXISTS productivity_dev.primaryissuance.loans_raw;
DROP TABLE IF EXISTS productivity_dev.primaryissuance.bonds_raw;
DROP TABLE IF EXISTS productivity_dev.primaryissuance.loans_raw_landing;
DROP TABLE IF EXISTS productivity_dev.primaryissuance.bonds_raw_landing;
```

## 5.3 Drop checkpoints and run tables

```sql
DROP TABLE IF EXISTS productivity_dev.primaryissuance.loans_ingestion_checkpoint;
DROP TABLE IF EXISTS productivity_dev.primaryissuance.bonds_ingestion_checkpoint;
DROP TABLE IF EXISTS productivity_dev.primaryissuance.loans_ingestion_runs;
DROP TABLE IF EXISTS productivity_dev.primaryissuance.bonds_ingestion_runs;
```

## 5.4 Drop operational transformation objects

```sql
DROP VIEW IF EXISTS productivity_dev.primaryissuance.deal_source_current;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.deals_source;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.deal_tranches_parsed;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.tranches_source;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.deal_participants_source;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.tranche_participant_candidates;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.tranche_participants_source;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.tranche_participant_roles_source;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.company_candidates;
DROP VIEW IF EXISTS productivity_dev.primaryissuance.companies_source;

DROP TABLE IF EXISTS productivity_dev.primaryissuance.deal_search_projection;
DROP TABLE IF EXISTS productivity_dev.primaryissuance.pipeline_watermarks;
```

Do not drop schemas unless specifically required.

---

# Part 6: Full Loan initial load from local Python

## 6.1 Set complete Loan environment

```powershell
$Env:INSTRUMENT_TYPE="Loan"
$Env:RAW_TABLE="loans_raw"
$Env:CHECKPOINT_TABLE="loans_ingestion_checkpoint"
$Env:RUN_TABLE="loans_ingestion_runs"
$Env:LOCAL_STAGING_DIR="$PWD\primary_issuance_v5_staging\loan"
$Env:MAX_PAGES="0"
```

## 6.2 Verify Loan configuration

```powershell
Write-Host "INSTRUMENT_TYPE=$Env:INSTRUMENT_TYPE"
Write-Host "RAW_TABLE=$Env:RAW_TABLE"
Write-Host "CHECKPOINT_TABLE=$Env:CHECKPOINT_TABLE"
Write-Host "RUN_TABLE=$Env:RUN_TABLE"
Write-Host "LOCAL_STAGING_DIR=$Env:LOCAL_STAGING_DIR"

if ($Env:INSTRUMENT_TYPE -ne "Loan") { throw "Expected Loan" }
if ($Env:RAW_TABLE -ne "loans_raw") { throw "Expected loans_raw" }
if ($Env:CHECKPOINT_TABLE -ne "loans_ingestion_checkpoint") { throw "Expected Loan checkpoint" }
if ($Env:RUN_TABLE -ne "loans_ingestion_runs") { throw "Expected Loan run table" }
```

## 6.3 Create Loan tables

```powershell
python primary_issuance_ingest_v5.py setup
```

## 6.4 Run full Loan initial extraction

```powershell
python primary_issuance_ingest_v5.py initial --reset
```

Do not run incremental until initial load completes.

## 6.5 Validate Loan raw table in Databricks

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT document_id) AS distinct_documents,
    COUNT(*) - COUNT(DISTINCT document_id) AS duplicate_rows
FROM productivity_dev.primaryissuance.loans_raw;
```

Expected:

```text
duplicate_rows = 0
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

---

# Part 7: Full Bond initial load from local Python

## 7.1 Set complete Bond environment

```powershell
$Env:INSTRUMENT_TYPE="Bond"
$Env:RAW_TABLE="bonds_raw"
$Env:CHECKPOINT_TABLE="bonds_ingestion_checkpoint"
$Env:RUN_TABLE="bonds_ingestion_runs"
$Env:LOCAL_STAGING_DIR="$PWD\primary_issuance_v5_staging\bond"
$Env:MAX_PAGES="0"
```

## 7.2 Verify Bond configuration

```powershell
Write-Host "INSTRUMENT_TYPE=$Env:INSTRUMENT_TYPE"
Write-Host "RAW_TABLE=$Env:RAW_TABLE"
Write-Host "CHECKPOINT_TABLE=$Env:CHECKPOINT_TABLE"
Write-Host "RUN_TABLE=$Env:RUN_TABLE"
Write-Host "LOCAL_STAGING_DIR=$Env:LOCAL_STAGING_DIR"

if ($Env:INSTRUMENT_TYPE -ne "Bond") { throw "Expected Bond" }
if ($Env:RAW_TABLE -ne "bonds_raw") { throw "Expected bonds_raw" }
if ($Env:CHECKPOINT_TABLE -ne "bonds_ingestion_checkpoint") { throw "Expected Bond checkpoint" }
if ($Env:RUN_TABLE -ne "bonds_ingestion_runs") { throw "Expected Bond run table" }
```

## 7.3 Create Bond tables

```powershell
python primary_issuance_ingest_v5.py setup
```

## 7.4 Run full Bond initial extraction

```powershell
python primary_issuance_ingest_v5.py initial --reset
```

## 7.5 Validate Bond raw table in Databricks

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT document_id) AS distinct_documents,
    COUNT(*) - COUNT(DISTINCT document_id) AS duplicate_rows
FROM productivity_dev.primaryissuance.bonds_raw;
```

Expected:

```text
duplicate_rows = 0
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

---

# Part 8: Build parsed model in Databricks

Run SQL files from clean-layout query package in this exact order: 

Notebook name : Primary Issuance Data Ingestion

URL : https://adb-1883700548140357.17.azuredatabricks.net/editor/notebooks/4128949757694498?o=1883700548140357 
```text
00_setup.sql
01_operational_source_views.sql
02_operational_tranche_views.sql
03_operational_participant_views.sql
04_initial_parsed_tables.sql
06_operational_search_projection.sql
07_validation.sql
```

## Parsed schema validation

```sql
SHOW TABLES IN productivity_dev.primaryissuance_parsed;
```

Expected parsed tables only:

```text
deals
tranches
companies
deal_participants
tranche_participants
tranche_participant_roles
```

## Parsed key validation

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT deal_key) AS distinct_keys,
    COUNT(*) - COUNT(DISTINCT deal_key) AS duplicates
FROM productivity_dev.primaryissuance_parsed.deals;
```

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT tranche_key) AS distinct_keys,
    COUNT(*) - COUNT(DISTINCT tranche_key) AS duplicates
FROM productivity_dev.primaryissuance_parsed.tranches;
```

Expected:

```text
duplicates = 0
```

## Domain coverage

```sql
SELECT instrument_type, COUNT(*) AS deals
FROM productivity_dev.primaryissuance_parsed.deals
GROUP BY instrument_type;
```

```sql
SELECT instrument_type, COUNT(*) AS tranches
FROM productivity_dev.primaryissuance_parsed.tranches
GROUP BY instrument_type;
```

Both should return Loan and Bond.

---

# Part 9: Final initial-load validation

## Raw isolation

```sql
SELECT 'loans_raw' AS table_name, instrument_type, COUNT(*) AS rows
FROM productivity_dev.primaryissuance.loans_raw
GROUP BY instrument_type
UNION ALL
SELECT 'bonds_raw', instrument_type, COUNT(*)
FROM productivity_dev.primaryissuance.bonds_raw
GROUP BY instrument_type;
```

Expected:

```text
loans_raw | Loan
bonds_raw | Bond
```

## Landing tables should be empty

```sql
SELECT 'loans_raw_landing' AS table_name, COUNT(*) AS rows
FROM productivity_dev.primaryissuance.loans_raw_landing
UNION ALL
SELECT 'bonds_raw_landing', COUNT(*)
FROM productivity_dev.primaryissuance.bonds_raw_landing;
```

Expected:

```text
0 rows in both landing tables
```

## Checkpoint review

```sql
SELECT *
FROM productivity_dev.primaryissuance.loans_ingestion_checkpoint
ORDER BY updated_at DESC;
```

```sql
SELECT *
FROM productivity_dev.primaryissuance.bonds_ingestion_checkpoint
ORDER BY updated_at DESC;
```

Initial checkpoints should contain `search_after_json` and correct domain.

---

# Part 10: After successful rebuild

Switch to daily runbook:

```text
DAILY_INCREMENTAL_RUN.md
```

Do not run `initial --reset` as part of normal daily process.
