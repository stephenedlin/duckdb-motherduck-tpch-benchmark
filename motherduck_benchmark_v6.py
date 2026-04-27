"""
=============================================================
 motherduck_benchmark_v5.py
 Research : Edge Analytics vs Cloud Analytics
 Author   : Stephen Edlin — PRADITA University
=============================================================
 HOW TO USE:

   1. Set your MotherDuck token as an environment variable:
        Windows:  set MOTHERDUCK_TOKEN=your_token_here
        Mac/Linux: export MOTHERDUCK_TOKEN=your_token_here

   2. Run:
        python motherduck_benchmark_v5.py

   The script will automatically:
     - Upload TPC-H data (skip tables that already exist)
     - Resume from where it left off if interrupted
     - Continue from run N+1 if CSV already has partial runs
     - Save all results to CSV after each run
     - Drop the database after each SF is complete

 OPTIONAL — run specific scale factors only:
   python motherduck_benchmark_v5.py --sf 1 5
   python motherduck_benchmark_v5.py --sf 10 20
   python motherduck_benchmark_v5.py --n 20          (target total run, default: 30)
   python motherduck_benchmark_v5.py --no-drop       (keep DB for debugging)
   python motherduck_benchmark_v5.py --log-file PATH (append ke log lama)

 RESUME (lanjut dari n=10 ke n=30):
   Script otomatis deteksi berapa run sudah ada di CSV.
   Hanya menjalankan sisa run yang belum ada.
   run_number di CSV akan lanjut dari 11, 12, ... 30.

 OUTPUT:
   results/motherduck_results.csv   — all benchmark results (append)
   results/logs/log_MotherDuck.log  — log lengkap (append setiap sesi)
=============================================================
"""

import duckdb
import time
import os
import csv
import gc
import sys
import socket
import argparse
import logging
from datetime import datetime

# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════

# Token is loaded from environment variable — do NOT hardcode here.
# Set it before running:
#   Windows:   set MOTHERDUCK_TOKEN=your_token_here
#   Mac/Linux: export MOTHERDUCK_TOKEN=your_token_here
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")

# Local TPC-H Parquet data folder
# Structure: DATA_DIR/sf1/lineitem.parquet, DATA_DIR/sf5/lineitem.parquet, etc.
DATA_DIR = r"C:\benchmark_stephen\data"

# Output folder
RESULTS_DIR = r"C:\benchmark_stephen\results"
OUTPUT_CSV  = os.path.join(RESULTS_DIR, "motherduck_results.csv")

# ── Benchmark parameters ─────────────────────────────────────
N_RUNS      = 30  # measured runs per query (target total, override via --n)
COOLING_SEC = 5   # seconds between runs

# ── TPC-H tables ─────────────────────────────────────────────
TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# Estimated storage size per SF (for display only)
SF_SIZE_MB = {1: 500, 5: 2500, 10: 5000, 20: 10000}

# ══════════════════════════════════════════════════════════════
#  15 TPC-H QUERIES
# ══════════════════════════════════════════════════════════════

QUERIES = {
    # ── Simple SELECT ────────────────────────────────────────
    "Q6_simple": {
        "category": "simple_select", "tpch_ref": "Q6",
        "sql": """
            SELECT SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE l_shipdate >= DATE '1994-01-01'
              AND l_shipdate  < DATE '1995-01-01'
              AND l_discount BETWEEN 0.05 AND 0.07
              AND l_quantity < 24;
        """,
    },
    "Q14_simple": {
        "category": "simple_select", "tpch_ref": "Q14",
        "sql": """
            SELECT
                100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                    THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM lineitem JOIN part ON l_partkey = p_partkey
            WHERE l_shipdate >= DATE '1995-09-01'
              AND l_shipdate  < DATE '1995-10-01';
        """,
    },
    "Q19_simple": {
        "category": "simple_select", "tpch_ref": "Q19",
        "sql": """
            SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM lineitem JOIN part ON p_partkey = l_partkey
            WHERE (
                p_brand = 'Brand#12' AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
                AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5
                AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'
            ) OR (
                p_brand = 'Brand#23' AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK')
                AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'
            ) OR (
                p_brand = 'Brand#34' AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG')
                AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'
            );
        """,
    },

    # ── JOIN ─────────────────────────────────────────────────
    "Q3_join": {
        "category": "join", "tpch_ref": "Q3",
        "sql": """
            SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
                   o_orderdate, o_shippriority
            FROM customer JOIN orders ON c_custkey=o_custkey
            JOIN lineitem ON l_orderkey=o_orderkey
            WHERE c_mktsegment='BUILDING' AND o_orderdate < DATE '1995-03-15'
              AND l_shipdate > DATE '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate LIMIT 10;
        """,
    },
    "Q5_join": {
        "category": "join", "tpch_ref": "Q5",
        "sql": """
            SELECT n_name, SUM(l_extendedprice*(1-l_discount)) AS revenue
            FROM customer JOIN orders ON c_custkey=o_custkey
            JOIN lineitem ON l_orderkey=o_orderkey
            JOIN supplier ON l_suppkey=s_suppkey
            JOIN nation ON c_nationkey=n_nationkey AND s_nationkey=n_nationkey
            JOIN region ON n_regionkey=r_regionkey
            WHERE r_name='ASIA' AND o_orderdate >= DATE '1994-01-01'
              AND o_orderdate < DATE '1995-01-01'
            GROUP BY n_name ORDER BY revenue DESC;
        """,
    },
    "Q10_join": {
        "category": "join", "tpch_ref": "Q10",
        "sql": """
            SELECT c_custkey, c_name, SUM(l_extendedprice*(1-l_discount)) AS revenue,
                   c_acctbal, n_name, c_address, c_phone, c_comment
            FROM customer JOIN orders ON c_custkey=o_custkey
            JOIN lineitem ON l_orderkey=o_orderkey
            JOIN nation ON c_nationkey=n_nationkey
            WHERE o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'
              AND l_returnflag='R'
            GROUP BY c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment
            ORDER BY revenue DESC LIMIT 20;
        """,
    },

    # ── Aggregation ──────────────────────────────────────────
    "Q1_agg": {
        "category": "aggregation", "tpch_ref": "Q1",
        "sql": """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity) AS sum_qty,
                   SUM(l_extendedprice) AS sum_base_price,
                   SUM(l_extendedprice*(1-l_discount)) AS sum_disc_price,
                   SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge,
                   AVG(l_quantity) AS avg_qty,
                   AVG(l_extendedprice) AS avg_price,
                   AVG(l_discount) AS avg_disc,
                   COUNT(*) AS count_order
            FROM lineitem WHERE l_shipdate <= DATE '1998-09-02'
            GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;
        """,
    },
    "Q4_agg": {
        "category": "aggregation", "tpch_ref": "Q4",
        "sql": """
            SELECT o_orderpriority, COUNT(*) AS order_count
            FROM orders
            WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01'
              AND EXISTS (SELECT 1 FROM lineitem
                          WHERE l_orderkey=o_orderkey AND l_commitdate < l_receiptdate)
            GROUP BY o_orderpriority ORDER BY o_orderpriority;
        """,
    },
    "Q7_agg": {
        "category": "aggregation", "tpch_ref": "Q7",
        "sql": """
            SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
            FROM (
                SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
                       EXTRACT(YEAR FROM l_shipdate) AS l_year,
                       l_extendedprice*(1-l_discount) AS volume
                FROM supplier JOIN lineitem ON s_suppkey=l_suppkey
                JOIN orders ON l_orderkey=o_orderkey
                JOIN customer ON o_custkey=c_custkey
                JOIN nation n1 ON s_nationkey=n1.n_nationkey
                JOIN nation n2 ON c_nationkey=n2.n_nationkey
                WHERE ((n1.n_name='FRANCE' AND n2.n_name='GERMANY')
                    OR (n1.n_name='GERMANY' AND n2.n_name='FRANCE'))
                  AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            ) AS shipping
            GROUP BY supp_nation, cust_nation, l_year
            ORDER BY supp_nation, cust_nation, l_year;
        """,
    },

    # ── Subquery ─────────────────────────────────────────────
    "Q17_subquery": {
        "category": "subquery", "tpch_ref": "Q17",
        "sql": """
            SELECT SUM(l_extendedprice)/7.0 AS avg_yearly
            FROM lineitem JOIN part ON p_partkey=l_partkey
            WHERE p_brand='Brand#23' AND p_container='MED BOX'
              AND l_quantity < (SELECT 0.2*AVG(l_quantity) FROM lineitem
                                WHERE l_partkey=p_partkey);
        """,
    },
    "Q20_subquery": {
        "category": "subquery", "tpch_ref": "Q20",
        "sql": """
            SELECT s_name, s_address FROM supplier
            JOIN nation ON s_nationkey=n_nationkey
            WHERE n_name='CANADA'
              AND s_suppkey IN (
                  SELECT ps_suppkey FROM partsupp
                  WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
                    AND ps_availqty > (
                        SELECT 0.5*SUM(l_quantity) FROM lineitem
                        WHERE l_partkey=ps_partkey AND l_suppkey=ps_suppkey
                          AND l_shipdate >= DATE '1994-01-01'
                          AND l_shipdate  < DATE '1995-01-01'))
            ORDER BY s_name;
        """,
    },
    "Q22_subquery": {
        "category": "subquery", "tpch_ref": "Q22",
        "sql": """
            SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
            FROM (
                SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
                FROM customer
                WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')
                  AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer
                                   WHERE c_acctbal>0.00
                                     AND SUBSTRING(c_phone FROM 1 FOR 2)
                                         IN ('13','31','23','29','30','18','17'))
                  AND NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey=c_custkey)
            ) AS custsale
            GROUP BY cntrycode ORDER BY cntrycode;
        """,
    },

    # ── Window Function ──────────────────────────────────────
    "Q10_window": {
        "category": "window_function", "tpch_ref": "Q10-W",
        "sql": """
            SELECT c_custkey, c_name, revenue,
                   RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
            FROM (
                SELECT c_custkey, c_name, SUM(l_extendedprice*(1-l_discount)) AS revenue
                FROM customer JOIN orders ON c_custkey=o_custkey
                JOIN lineitem ON l_orderkey=o_orderkey
                JOIN nation ON c_nationkey=n_nationkey
                WHERE o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'
                  AND l_returnflag='R'
                GROUP BY c_custkey, c_name
            ) AS sub ORDER BY revenue_rank LIMIT 20;
        """,
    },
    "Q1_window": {
        "category": "window_function", "tpch_ref": "Q1-W",
        "sql": """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity) AS sum_qty,
                   SUM(l_extendedprice) AS sum_base_price,
                   COUNT(*) AS count_order,
                   SUM(SUM(l_quantity)) OVER (
                       ORDER BY l_returnflag, l_linestatus
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS running_qty
            FROM lineitem WHERE l_shipdate <= DATE '1998-09-02'
            GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;
        """,
    },
    "Q18_window": {
        "category": "window_function", "tpch_ref": "Q18-W",
        "sql": """
            SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
                   total_quantity,
                   RANK() OVER (PARTITION BY c_custkey ORDER BY total_quantity DESC) AS qty_rank,
                   ROW_NUMBER() OVER (ORDER BY total_quantity DESC) AS global_row_num
            FROM (
                SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
                       SUM(l_quantity) AS total_quantity
                FROM customer JOIN orders ON c_custkey=o_custkey
                JOIN lineitem ON o_orderkey=l_orderkey
                GROUP BY c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice
                HAVING SUM(l_quantity) > 300
            ) AS sub ORDER BY total_quantity DESC LIMIT 100;
        """,
    },
}


# ══════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════

def setup_logging(results_dir: str, scale_factors: list,
                  existing_log: str = None) -> str:
    log_dir = os.path.join(results_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    if existing_log:
        log_path = existing_log
    else:
        log_path = os.path.join(log_dir, "log_MotherDuck.log")
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s  %(message)s", datefmt="%H:%M:%S")
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return log_path


# ══════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════

def banner(msg: str, char: str = "="):
    line = char * 62
    logging.info("")
    logging.info(line)
    logging.info(f"  {msg}")
    logging.info(line)


def log(msg: str):
    logging.info(f"  {msg}")


def connect_md(token: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(f"md:?motherduck_token={token}")


def measure_rtt() -> float:
    try:
        t = time.perf_counter()
        s = socket.create_connection(("app.motherduck.com", 443), timeout=5)
        rtt = (time.perf_counter() - t) * 1000
        s.close()
        return round(rtt, 2)
    except Exception:
        return -1.0


def db_name(sf: int) -> str:
    return f"tpch_sf{sf}"


# ── Database / table checks ───────────────────────────────────

def db_exists(con, sf: int) -> bool:
    try:
        rows = con.execute("SHOW DATABASES").fetchall()
        return any(r[0] == db_name(sf) for r in rows)
    except Exception:
        return False


def table_exists(con, db: str, table: str) -> bool:
    try:
        r = con.execute(
            f"SELECT COUNT(*) FROM {db}.information_schema.tables "
            f"WHERE table_name = '{table}'"
        ).fetchone()
        return r[0] > 0
    except Exception:
        return False


def get_row_count(con, db: str, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {db}.main.{table}").fetchone()[0]
    except Exception:
        return -1


def is_db_complete(con, sf: int) -> bool:
    if not db_exists(con, sf):
        return False
    db = db_name(sf)
    for table in TPCH_TABLES:
        if not table_exists(con, db, table):
            return False
        if get_row_count(con, db, table) <= 0:
            return False
    return True


# ── CSV helpers ───────────────────────────────────────────────

def load_completed(filepath: str, n_runs: int) -> set:
    completed = set()
    if not os.path.exists(filepath):
        return completed
    counts = {}
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("run_number", "") == "ERROR":
                continue
            try:
                key = (row["query_name"], int(row["scale_factor"]))
                counts[key] = counts.get(key, 0) + 1
            except (KeyError, ValueError):
                continue
    for key, count in counts.items():
        if count >= n_runs:
            completed.add(key)
    return completed


def count_existing_runs(filepath: str, query_name: str, sf: int) -> int:
    """Hitung berapa run sudah ada di CSV untuk query + SF tertentu."""
    if not os.path.exists(filepath):
        return 0
    count = 0
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("run_number", "") == "ERROR":
                continue
            try:
                if (row["query_name"] == query_name
                        and int(row["scale_factor"]) == sf):
                    count += 1
            except (KeyError, ValueError):
                continue
    return count


def sf_fully_done(completed: set, sf: int) -> bool:
    return all((qname, sf) in completed for qname in QUERIES)


def write_csv_header(filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not os.path.exists(filepath):
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "platform", "query_name", "tpch_ref", "category",
                "scale_factor", "run_number", "exec_time_s",
                "mean_time_s", "std_time_s", "min_time_s", "max_time_s",
                "memory_before_mb", "memory_after_mb", "memory_delta_mb",
                "network_rtt_ms", "timestamp",
            ])


def safe_append_csv(filepath: str, rows: list):
    for attempt in range(1, 6):
        try:
            with open(filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow(row)
            return
        except PermissionError:
            if attempt < 5:
                log(f"[!] CSV locked. Retrying {attempt}/5 in 3 seconds...")
                time.sleep(3)
            else:
                raise


def append_result(filepath, query_name, query_info, sf, result, rtt_ms,
                  start_run: int = 1):
    """Tulis hasil ke CSV. start_run agar run_number lanjut dari yang sudah ada."""
    rows = []
    for i, t in enumerate(result["times"], start=start_run):
        rows.append([
            "motherduck",
            query_name, query_info["tpch_ref"], query_info["category"],
            sf, i,
            round(t, 6), round(result["mean"], 6), round(result["std"], 6),
            round(result["min"], 6), round(result["max"], 6),
            0, 0, 0,
            rtt_ms,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ])
    safe_append_csv(filepath, rows)


# ══════════════════════════════════════════════════════════════
#  UPLOAD — smart check, skip existing tables
# ══════════════════════════════════════════════════════════════

def upload_sf(con, sf: int) -> bool:
    db     = db_name(sf)
    sf_dir = os.path.join(DATA_DIR, f"sf{sf}")

    if not os.path.exists(sf_dir):
        log(f"[ERROR] Data folder not found: {sf_dir}")
        return False

    if is_db_complete(con, sf):
        log(f"SF{sf}: Database '{db}' already complete — skipping upload.")
        return True

    try:
        con.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        log(f"Database '{db}' ready.")
    except Exception as e:
        log(f"[WARN] CREATE DATABASE: {e}")

    size_mb = SF_SIZE_MB.get(sf, 0)
    log(f"Estimated size SF{sf}: ~{size_mb} MB — uploading missing tables...")
    logging.info("")

    all_ok = True
    for table in TPCH_TABLES:
        parquet_path = os.path.join(sf_dir, f"{table}.parquet")

        if not os.path.exists(parquet_path):
            log(f"[ERROR] File not found: {parquet_path}")
            all_ok = False
            continue

        file_mb = os.path.getsize(parquet_path) / (1024 * 1024)

        if table_exists(con, db, table) and get_row_count(con, db, table) > 0:
            rows = get_row_count(con, db, table)
            logging.info(f"    [SKIP] {table:12s}  {file_mb:7.1f} MB  already exists ({rows:,} rows)")
            continue

        logging.info(f"    [UP]   {table:12s}  {file_mb:7.1f} MB  uploading...")
        t0       = time.perf_counter()
        sql_path = parquet_path.replace("\\", "/")

        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {db}.main.{table} AS
                SELECT * FROM read_parquet('{sql_path}')
            """)
            elapsed = time.perf_counter() - t0
            rows    = get_row_count(con, db, table)
            logging.info(f"    [OK]   {table:12s}  done in {elapsed:.1f}s  ({rows:,} rows)")
        except Exception as e:
            logging.info(f"    [FAIL] {table:12s}  {e}")
            all_ok = False

    logging.info("")
    return all_ok


def verify_sf(con, sf: int) -> bool:
    db = db_name(sf)
    log(f"Verifying tables for SF{sf}...")
    all_ok = True
    for table in TPCH_TABLES:
        rows   = get_row_count(con, db, table)
        status = "OK" if rows > 0 else "FAIL"
        logging.info(f"      [{status}] {table:12s}  {rows:>12,} rows")
        if rows <= 0:
            all_ok = False
    logging.info("")
    return all_ok


# ══════════════════════════════════════════════════════════════
#  BENCHMARK
# ══════════════════════════════════════════════════════════════

def setup_views(con, sf: int):
    db = db_name(sf)
    for table in TPCH_TABLES:
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM {db}.main.{table}"
        )


def run_query(con, query_name: str, query_info: dict,
              n_runs: int, skip_warmup: bool = False) -> dict:
    sql = query_info["sql"]

    if not skip_warmup:
        logging.info(f"      Warm-up...")
        con.execute(sql).fetchall()
        logging.info(f"      Warm-up done.")
    else:
        logging.info(f"      Warm-up skipped (resume mode)")

    times = []
    for run_num in range(1, n_runs + 1):
        t0      = time.perf_counter()
        result  = con.execute(sql).fetchall()
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        logging.info(f"      Run {run_num:02d}/{n_runs}: {elapsed:.4f}s  ({len(result)} rows)")

        if run_num < n_runs:
            time.sleep(COOLING_SEC)

    mean = sum(times) / len(times)
    std  = (sum((t - mean) ** 2 for t in times) / len(times)) ** 0.5

    return {
        "times": times,
        "mean":  mean,
        "std":   std,
        "min":   min(times),
        "max":   max(times),
    }


def benchmark_sf(con, sf: int, completed: set, rtt_ms: float, n_runs: int) -> int:
    log(f"Setting up views for SF{sf}...")
    setup_views(con, sf)
    log(f"Views ready. Starting benchmark: {len(QUERIES)} queries x {n_runs} runs...\n")

    done = 0
    for q_num, (query_name, query_info) in enumerate(QUERIES.items(), start=1):
        key = (query_name, sf)

        if key in completed:
            logging.info(f"  [{q_num:02d}/{len(QUERIES)}] {query_name} SF{sf} — SKIP (already {n_runs} runs in CSV)")
            continue

        # Cek berapa run sudah ada (resume parsial, misal 10 → 30)
        existing  = count_existing_runs(OUTPUT_CSV, query_name, sf)
        remaining = n_runs - existing

        if existing > 0:
            logging.info(f"  [{q_num:02d}/{len(QUERIES)}] {query_name} SF{sf} "
                         f"— RESUME from run {existing+1} (have {existing}/{n_runs})")
        else:
            logging.info(f"  [{q_num:02d}/{len(QUERIES)}] {query_name} "
                         f"[{query_info['category']}] — {query_info['tpch_ref']}")

        try:
            result = run_query(con, query_name, query_info,
                               remaining, skip_warmup=(existing > 0))
            append_result(OUTPUT_CSV, query_name, query_info, sf,
                          result, rtt_ms, start_run=existing + 1)
            completed.add(key)
            done += 1
            logging.info(f"      DONE  MEAN={result['mean']:.4f}s  STD={result['std']:.4f}s  "
                         f"MIN={result['min']:.4f}s  MAX={result['max']:.4f}s\n")

        except Exception as e:
            logging.info(f"      ERROR: {e}")
            log("Reconnecting to MotherDuck...")
            try:
                con.close()
            except Exception:
                pass
            try:
                con = connect_md(MOTHERDUCK_TOKEN)
                setup_views(con, sf)
                log("Reconnect successful.")
            except Exception as e2:
                log(f"Reconnect FAILED: {e2}")
                break

    logging.info("")
    return done


# ══════════════════════════════════════════════════════════════
#  DROP DATABASE (cleanup after each SF)
# ══════════════════════════════════════════════════════════════

def drop_sf(con, sf: int):
    db = db_name(sf)
    log(f"Dropping database '{db}' from MotherDuck (post-benchmark cleanup)...")
    try:
        con.execute(f"DROP DATABASE IF EXISTS {db}")
        log(f"'{db}' dropped successfully.")
    except Exception as e:
        log(f"[WARN] Failed to drop '{db}': {e}")


# ══════════════════════════════════════════════════════════════
#  PROGRESS DISPLAY
# ══════════════════════════════════════════════════════════════

def print_progress(completed: set, scale_factors: list):
    logging.info(f"\n  {'─'*58}")
    logging.info(f"  PROGRESS")
    logging.info(f"  {'─'*58}")
    for sf in scale_factors:
        sf_done  = sum(1 for qname in QUERIES if (qname, sf) in completed)
        sf_total = len(QUERIES)
        bar_fill = int((sf_done / sf_total) * 24)
        bar      = "█" * bar_fill + "░" * (24 - bar_fill)
        status   = "DONE" if sf_done == sf_total else f"{sf_done}/{sf_total}"
        logging.info(f"  SF{sf:2d}  [{bar}]  {status}")
    total_done = sum(1 for qname in QUERIES for sf in scale_factors if (qname, sf) in completed)
    total_all  = len(QUERIES) * len(scale_factors)
    logging.info(f"  {'─'*58}")
    logging.info(f"  Total: {total_done}/{total_all} queries complete\n")


# ══════════════════════════════════════════════════════════════
#  TOKEN VALIDATION
# ══════════════════════════════════════════════════════════════

def validate_token():
    if not MOTHERDUCK_TOKEN or len(MOTHERDUCK_TOKEN) < 20:
        logging.info("\n  [ERROR] MOTHERDUCK_TOKEN is not set.")
        logging.info("  Set it before running:")
        logging.info("    Windows:   set MOTHERDUCK_TOKEN=your_token_here")
        logging.info("    Mac/Linux: export MOTHERDUCK_TOKEN=your_token_here")
        sys.exit(1)


def test_connection() -> duckdb.DuckDBPyConnection:
    log("Testing connection to MotherDuck...")
    try:
        con = connect_md(MOTHERDUCK_TOKEN)
        con.execute("SELECT 1").fetchone()
        log("Connection OK.")
        return con
    except Exception as e:
        log(f"Connection FAILED: {e}")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="MotherDuck Benchmark v4 — Stephen Edlin (PRADITA University)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python motherduck_benchmark_v4.py              # run all SF
  python motherduck_benchmark_v4.py --sf 1 5     # run SF1 and SF5 only
  python motherduck_benchmark_v4.py --sf 10 20   # run SF10 and SF20 only
  python motherduck_benchmark_v4.py --no-drop    # keep DB after benchmark (debugging)
        """
    )
    parser.add_argument(
        "--sf", nargs="+", type=int,
        choices=[1, 5, 10, 20],
        default=[1, 5, 10, 20],
        metavar="N",
        help="Scale factors to benchmark. Default: all (1 5 10 20)."
    )
    parser.add_argument(
        "--no-drop", action="store_true",
        help="Do not drop database after benchmark (for debugging)."
    )
    parser.add_argument(
        "--n", type=int, default=None,
        metavar="RUNS",
        help=f"Target total run per query per SF. Default: {N_RUNS}. "
             "Contoh: --n 20 -> jalankan sampai 20 run total. "
             "Kalau CSV sudah punya 10 run, hanya akan menjalankan 10 run lagi."
    )
    parser.add_argument(
        "--log-file", type=str, default=None,
        metavar="PATH",
        help="Append ke file log yang sudah ada (misal log dari sesi n=10). "
             "Contoh: --log-file \"C:\\benchmark_stephen\\results\\logs\\log_MotherDuck.log\""
    )
    return parser.parse_args()


if __name__ == "__main__":

    args          = parse_args()
    scale_factors = sorted(set(args.sf))
    keep_db       = args.no_drop
    existing_log  = args.log_file

    # Resolve n_runs: --n override N_RUNS, tapi tidak boleh < 1
    n_runs = args.n if args.n is not None else N_RUNS
    if n_runs < 1:
        print(f"[ERROR] --n harus >= 1 (dapat: {n_runs})")
        sys.exit(1)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    log_path = setup_logging(RESULTS_DIR, scale_factors, existing_log)

    banner("MotherDuck Benchmark v5 — Stephen Edlin (PRADITA University)")
    logging.info(f"  Token      : ***...{MOTHERDUCK_TOKEN[-6:] if len(MOTHERDUCK_TOKEN) > 6 else '(not set)'}")
    logging.info(f"  Data dir   : {DATA_DIR}")
    logging.info(f"  Output CSV : {OUTPUT_CSV}")
    logging.info(f"  Log file   : {log_path} (APPEND)")
    logging.info(f"  Scale factors    : {scale_factors}")
    logging.info(f"  Runs per query   : {n_runs} (target total per query per SF)")
    logging.info(f"  Cooling interval : {COOLING_SEC}s between runs")
    logging.info(f"  Drop DB after SF : {'NO (--no-drop)' if keep_db else 'YES'}")
    logging.info(f"  Resume mode      : script will count existing runs and continue from N+1")

    # ── Validate token ──────────────────────────────────────
    validate_token()

    # ── Validate data directory ─────────────────────────────
    if not os.path.exists(DATA_DIR):
        logging.info(f"\n  [ERROR] DATA_DIR not found: {DATA_DIR}")
        logging.info(f"  Make sure your TPC-H Parquet files are at that path.")
        sys.exit(1)

    # ── Test connection ─────────────────────────────────────
    logging.info("")
    con = test_connection()
    logging.info("")

    # ── CSV setup ───────────────────────────────────────────
    write_csv_header(OUTPUT_CSV)
    completed = load_completed(OUTPUT_CSV, n_runs)

    if completed:
        already = sum(1 for qname in QUERIES for sf in scale_factors if (qname, sf) in completed)
        logging.info(f"  Resume mode: {already} queries for SF{scale_factors} already complete ({n_runs} runs).")
        logging.info(f"  Partial queries will be continued from their next run number.\n")

    print_progress(completed, scale_factors)

    grand_start = time.perf_counter()
    grand_done  = 0

    # ── Main loop per SF ────────────────────────────────────
    for sf in scale_factors:

        if sf_fully_done(completed, sf):
            log(f"SF{sf} already complete ({len(QUERIES)} queries x {n_runs} runs) — skipping.")
            continue

        banner(f"SCALE FACTOR {sf}  (~{SF_SIZE_MB.get(sf,0)//1024} GB)", char="-")

        rtt_ms = measure_rtt()
        log(f"Network RTT to MotherDuck: {rtt_ms:.1f} ms")

        gc.collect()

        # Fresh connection per SF
        try:
            con.close()
        except Exception:
            pass
        con = connect_md(MOTHERDUCK_TOKEN)

        # Phase 1: Upload
        log(f"Phase 1: Check & upload data for SF{sf}...")
        if not upload_sf(con, sf):
            log(f"[ERROR] Upload failed for SF{sf} — skipping.")
            continue

        # Phase 2: Verify
        log(f"Phase 2: Verifying SF{sf} data...")
        if not verify_sf(con, sf):
            log(f"[ERROR] Verification failed for SF{sf} — skipping.")
            continue

        # Phase 3: Benchmark
        log(f"Phase 3: Benchmarking SF{sf}...")
        done_count = benchmark_sf(con, sf, completed, rtt_ms, n_runs)
        grand_done += done_count

        # Phase 4: Drop DB
        if sf_fully_done(completed, sf):
            log(f"SF{sf}: All {len(QUERIES)} queries complete ({n_runs} runs each).")
            if not keep_db:
                drop_sf(con, sf)
            else:
                log(f"--no-drop active, database SF{sf} retained.")
        else:
            missing = sum(1 for qname in QUERIES if (qname, sf) not in completed)
            log(f"[WARN] SF{sf}: {missing} queries incomplete — database NOT dropped.")
            log(f"       Re-run the same command to resume.")

        print_progress(completed, scale_factors)

        # Inter-SF pause
        idx = scale_factors.index(sf)
        if idx < len(scale_factors) - 1:
            next_sf = scale_factors[idx + 1]
            if not sf_fully_done(completed, next_sf):
                log(f"Waiting 10 seconds before SF{next_sf}...")
                time.sleep(10)

    # ── Done ────────────────────────────────────────────────
    try:
        con.close()
    except Exception:
        pass

    grand_elapsed = time.perf_counter() - grand_start

    banner("BENCHMARK COMPLETE!")
    logging.info(f"  Queries completed this session : {grand_done}")
    logging.info(f"  Total elapsed time             : {grand_elapsed/3600:.2f} hours ({grand_elapsed/60:.1f} minutes)")
    logging.info(f"  Output CSV                     : {OUTPUT_CSV}")
    logging.info(f"  Log file                       : {log_path}")
    logging.info(f"  Finished at                    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("")
    logging.info("  Output files ready for analysis:")
    logging.info("    duckdb_results.csv        <- local edge data")
    logging.info("    motherduck_results.csv    <- cloud data (this file)")
    logging.info("")
    logging.info("  Next step: merge both CSVs in your analysis notebook.")
    logging.info("=" * 62 + "\n")
