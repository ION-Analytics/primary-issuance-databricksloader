import argparse, gzip, hashlib, json, logging, os, signal, sys, uuid
from datetime import datetime, timezone
from pathlib import Path
import requests
from databricks import sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("primary_issuance_v5")
STOP = False

def stop(*_):
    global STOP
    STOP = True
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

ES_PROXY_URL = os.getenv("ES_PROXY_URL", "http://localhost:8090").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "deal_documents_latest")
INSTRUMENT_TYPE = os.getenv("INSTRUMENT_TYPE", "Loan")
RAW_TABLE = os.getenv("RAW_TABLE", "loans_raw")
CHECKPOINT_TABLE = os.getenv("CHECKPOINT_TABLE", "loans_ingestion_checkpoint")
RUN_TABLE = os.getenv("RUN_TABLE", "loans_ingestion_runs")
CATALOG = os.getenv("DB_CATALOG", "productivity_dev")
SCHEMA = os.getenv("DB_SCHEMA", "primaryissuance")
RAW = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
LANDING = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}_landing"
CHECKPOINT = f"{CATALOG}.{SCHEMA}.{CHECKPOINT_TABLE}"
RUNS = f"{CATALOG}.{SCHEMA}.{RUN_TABLE}"
VOLUME = os.getenv("VOLUME_PATH", "/Volumes/productivity_dev/primaryissuance/primaryissuance_elasticsearch").rstrip("/")
LOCAL = Path(os.getenv("LOCAL_STAGING_DIR", f"./primary_issuance_v5_staging/{INSTRUMENT_TYPE.lower()}")).resolve()
BATCH = int(os.getenv("ES_BATCH_SIZE", "5000"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "0"))
VERSION = "5.1"

def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def iso(value):
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if value else None

def parse_dt(value):
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None)
    text = str(value).strip()
    text = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError:
        return None

def validate():
    missing = [x for x in ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN") if not os.getenv(x)]
    if missing:
        raise RuntimeError("Missing env: " + ", ".join(missing))
    valid = {
        "Loan": ("loans_raw", "loans_ingestion_checkpoint", "loans_ingestion_runs"),
        "Bond": ("bonds_raw", "bonds_ingestion_checkpoint", "bonds_ingestion_runs"),
    }
    actual = (RAW_TABLE, CHECKPOINT_TABLE, RUN_TABLE)
    if INSTRUMENT_TYPE not in valid or actual != valid[INSTRUMENT_TYPE]:
        raise RuntimeError(f"Unsafe domain config: instrument={INSTRUMENT_TYPE}, tables={actual}, expected={valid.get(INSTRUMENT_TYPE)}")

def connect():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
        use_kernel=True,
    )

def execute(conn, statement, params=None):
    with conn.cursor() as cur:
        cur.execute(statement, params or [])

def fetch(conn, statement, params=None):
    with conn.cursor() as cur:
        cur.execute(statement, params or [])
        return cur.fetchall()

def setup(conn):
    columns = """instrument_type STRING, document_id STRING, last_modified_date STRING,
    document_version BIGINT, source_hash STRING, source_json STRING, es_index STRING,
    ingested_at STRING, ingestion_run_id STRING, pipeline_version STRING"""
    execute(conn, f"CREATE TABLE IF NOT EXISTS {RAW} ({columns}) USING DELTA")
    execute(conn, f"CREATE TABLE IF NOT EXISTS {LANDING} ({columns}) USING DELTA")
    execute(conn, f"""CREATE TABLE IF NOT EXISTS {CHECKPOINT} (
        instrument_type STRING, mode STRING, last_modified_date STRING, last_es_id STRING,
        documents_processed BIGINT, search_after_json STRING, updated_at TIMESTAMP,
        pipeline_version STRING) USING DELTA""")
    execute(conn, f"""CREATE TABLE IF NOT EXISTS {RUNS} (
        run_id STRING, instrument_type STRING, mode STRING, status STRING,
        started_at STRING, finished_at STRING, documents_read BIGINT,
        documents_written BIGINT, error_message STRING, pipeline_version STRING) USING DELTA""")
    LOCAL.mkdir(parents=True, exist_ok=True)

def raw_max(conn):
    rows = fetch(conn, f"SELECT MAX(try_cast(last_modified_date AS TIMESTAMP)) FROM {RAW} WHERE instrument_type=?", [INSTRUMENT_TYPE])
    return rows[0][0] if rows and rows[0][0] else None

def get_checkpoint(conn, mode):
    rows = fetch(conn, f"""SELECT last_modified_date,last_es_id,documents_processed,search_after_json
        FROM {CHECKPOINT} WHERE instrument_type=? AND mode=? ORDER BY updated_at DESC LIMIT 1""", [INSTRUMENT_TYPE, mode])
    if not rows:
        return None
    return parse_dt(rows[0][0]), str(rows[0][1] or ""), int(rows[0][2] or 0), json.loads(rows[0][3]) if rows[0][3] else None

def save_checkpoint(
    conn,
    mode,
    checkpoint_dt,
    last_id,
    processed,
    cursor,
):
    execute(
        conn,
        f"""
        DELETE FROM {CHECKPOINT}
        WHERE instrument_type = ?
          AND mode = ?
        """,
        [
            INSTRUMENT_TYPE,
            mode,
        ],
    )

    execute(
        conn,
        f"""
        INSERT INTO {CHECKPOINT}
        (
            instrument_type,
            mode,
            last_modified_date,
            last_es_id,
            documents_processed,
            search_after_json,
            updated_at,
            pipeline_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            INSTRUMENT_TYPE,
            mode,
            iso(checkpoint_dt),
            last_id,
            processed,
            json.dumps(cursor),
            now(),
            VERSION,
        ],
    )

def es_page(start_dt, search_after):
    must = [{"exists": {"field": "id"}}, {"match_phrase": {"instrumentType": INSTRUMENT_TYPE}}]
    if start_dt:
        must.append({"range": {"lastModifiedDate": {"gt": iso(start_dt)}}})
    body = {
        "size": BATCH,
        "track_total_hits": False,
        "query": {"bool": {"must": must}},
        "sort": [{"lastModifiedDate": {"order": "asc", "missing": "_last"}}, {"id": {"order": "asc"}}],
    }
    if search_after:
        body["search_after"] = search_after
    response = requests.get(f"{ES_PROXY_URL}/{ES_INDEX}/_search", json=body, timeout=(30, 300))
    if not response.ok:
        log.error("ES RESPONSE: %s", response.text[:10000])
        log.error("ES REQUEST: %s", json.dumps(body))
        response.raise_for_status()
    return response.json().get("hits", {}).get("hits", [])

def make_file(hits, page, run_id):
    path = LOCAL / f"{INSTRUMENT_TYPE.lower()}_{run_id}_{page:08d}_{uuid.uuid4().hex}.json.gz"
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as handle:
        for hit in hits:
            source = hit.get("_source") or {}
            if str(source.get("instrumentType", "")).lower() != INSTRUMENT_TYPE.lower():
                continue
            document_id = source.get("id")
            if document_id is None:
                continue
            source_json = json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            row = {
                "instrument_type": INSTRUMENT_TYPE,
                "document_id": str(document_id),
                "last_modified_date": iso(parse_dt(source.get("lastModifiedDate"))),
                "document_version": int(source["version"]) if source.get("version") is not None else None,
                "source_hash": hashlib.sha256(source_json.encode()).hexdigest(),
                "source_json": source_json,
                "es_index": ES_INDEX,
                "ingested_at": iso(now()),
                "ingestion_run_id": run_id,
                "pipeline_version": VERSION,
            }
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
    return path, count

def upload(local_path, remote_path):
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    token = os.getenv("DATABRICKS_TOKEN")
    with open(local_path, "rb") as handle:
        response = requests.put(
            f"https://{host}/api/2.0/fs/files{remote_path}?overwrite=true",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"},
            data=handle,
            timeout=600,
        )
    if response.status_code not in (200, 201, 204):
        raise RuntimeError(f"Upload failed {response.status_code}: {response.text}")

def landing_merge(conn, local_path, run_id):
    remote = f"{VOLUME}/{local_path.name}"
    upload(str(local_path), remote)
    execute(conn, f"COPY INTO {LANDING} FROM '{remote.replace(chr(39), chr(39)*2)}' FILEFORMAT=JSON FORMAT_OPTIONS('multiLine'='false')")
    execute(conn, f"""MERGE INTO {RAW} t USING (
        SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY instrument_type,document_id
                ORDER BY try_cast(last_modified_date AS TIMESTAMP) DESC NULLS LAST,
                         document_version DESC NULLS LAST,
                         try_cast(ingested_at AS TIMESTAMP) DESC
            ) rn
            FROM {LANDING} WHERE ingestion_run_id=?
        ) WHERE rn=1
    ) s
    ON t.instrument_type=s.instrument_type AND t.document_id=s.document_id
    WHEN MATCHED AND NOT(t.source_hash <=> s.source_hash) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""", [run_id])
    execute(conn, f"DELETE FROM {LANDING} WHERE ingestion_run_id=?", [run_id])

def run(mode, reset=False):
    validate()
    conn = connect()
    run_id = str(uuid.uuid4())
    total = 0
    try:
        setup(conn)
        if reset:
            execute(conn, f"DELETE FROM {CHECKPOINT} WHERE instrument_type=? AND mode=?", [INSTRUMENT_TYPE, mode])
        execute(conn, f"INSERT INTO {RUNS}(run_id,instrument_type,mode,status,started_at,documents_read,documents_written,pipeline_version) VALUES(?,?,?,'RUNNING',?,0,0,?)", [run_id, INSTRUMENT_TYPE, mode, iso(now()), VERSION])
        checkpoint = get_checkpoint(conn, mode)
        start_dt = None
        search_after = None
        processed = 0
        if mode == "initial":
            if checkpoint:
                _, _, processed, search_after = checkpoint
                log.info("Resuming initial using exact cursor: %s", search_after)
            else:
                log.info("Starting full initial load")
        else:
            start_dt = checkpoint[0] if checkpoint else raw_max(conn)
            if start_dt is None:
                raise RuntimeError("Incremental refused: no dated raw data. Run initial first.")
            log.info("Incremental strictly after %s; no overlap and no historical scan", start_dt)
        checkpoint_dt = start_dt
        page = 0
        while not STOP:
            page += 1
            if MAX_PAGES and page > MAX_PAGES:
                break
            hits = es_page(start_dt, search_after)
            if not hits:
                break
            cursor = hits[-1].get("sort")
            if not cursor:
                raise RuntimeError("Missing Elasticsearch sort cursor")
            local_path, count = make_file(hits, page, run_id)
            landing_merge(conn, local_path, run_id)
            local_path.unlink(missing_ok=True)
            total += count
            processed += len(hits)
            search_after = cursor
            dates = [parse_dt((hit.get("_source") or {}).get("lastModifiedDate")) for hit in hits]
            dates = [date for date in dates if date]
            if dates:
                checkpoint_dt = max(dates)
            last_id = str((hits[-1].get("_source") or {}).get("id", ""))
            save_checkpoint(conn, mode, checkpoint_dt, last_id, processed, cursor)
            log.info("Page %s fetched=%s merged=%s checkpoint=%s", page, len(hits), count, checkpoint_dt)
        execute(conn, f"UPDATE {RUNS} SET status='SUCCESS',finished_at=?,documents_read=?,documents_written=? WHERE run_id=?", [iso(now()), total, total, run_id])
    except Exception as exc:
        try:
            execute(conn, f"UPDATE {RUNS} SET status='FAILED',finished_at=?,error_message=? WHERE run_id=?", [iso(now()), str(exc)[:4000], run_id])
        except Exception:
            pass
        raise
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("setup")
    initial = commands.add_parser("initial")
    initial.add_argument("--reset", action="store_true")
    commands.add_parser("incremental")
    args = parser.parse_args()
    validate()
    if args.command == "setup":
        conn = connect()
        try:
            setup(conn)
        finally:
            conn.close()
    elif args.command == "initial":
        run("initial", args.reset)
    else:
        run("incremental")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("FAILED: %s", exc)
        sys.exit(1)
