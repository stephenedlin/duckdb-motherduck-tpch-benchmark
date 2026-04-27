"""
=============================================================
 duckdb_local_benchmark_v2.py
 Penelitian: Edge Analytics vs Cloud Analytics
 Peneliti  : Stephen Edlin — PRADITA University
=============================================================
 CARA PAKAI:

   python duckdb_local_benchmark_v2.py

 OPSIONAL:
   --sf 1 5 10 20       → pilih SF tertentu (default: semua)
   --n 20               → target total run (default: 30). Contoh: kalau CSV
                          sudah punya 10 run, --n 20 hanya jalankan 10 lagi
   --cooling 30         → detik cooling antar run (default: 30)
   --no-cache-flush     → skip flush OS page cache antar SF
   --sf-dir "D:\\data"  → override folder data TPC-H
   --log-file "path"    → append ke file log yang sudah ada

 ALUR:
   1. Cek file Parquet lokal sudah ada & valid
   2. Benchmark: 1 warm-up + N_RUNS measured runs per query
   3. Cooling 30 detik antar run (CPU settle)
   4. Flush OS page cache antar SF transition
   5. Simpan ke CSV (append-safe, resume otomatis)
   6. Log dual: terminal + file (append ke log lama kalau --log-file)

 RESUME (termasuk lanjut dari n=10 ke n=30):
   Script otomatis deteksi berapa run sudah ada di CSV.
   Hanya menjalankan sisa run yang belum ada.
   Contoh: CSV sudah punya 10 run, N_RUNS=30 → jalankan 20 run lagi.
   run_number di CSV akan lanjut dari 11, 12, ... 30.

 OUTPUT:
   duckdb_results.csv          → hasil benchmark (append ke yang lama)
   logs/log_DuckDB.log         → log lengkap (append setiap sesi)

 CATATAN HARDWARE:
   Script ini dirancang untuk:
     CPU  : Intel Core i5-12400F (6C/12T)
     RAM  : 16 GB DDR4 Dual-Channel
     SSD  : NVMe PCIe Gen3
     OS   : Windows 11
   Cooling 30 detik default cocok untuk CPU kelas ini agar
   tidak ada carry-over thermal/cache antar run.
=============================================================
"""

import duckdb
import time
import os
import csv
import gc
import sys
import ctypes
import argparse
import logging
import platform
from datetime import datetime

# ══════════════════════════════════════════════════════════════
#  KONFIGURASI — EDIT SESUAI SETUP KAMU
# ══════════════════════════════════════════════════════════════

# Folder data TPC-H Parquet lokal
# Struktur: DATA_DIR/sf1/lineitem.parquet, sf5/lineitem.parquet, dst.
DATA_DIR = r"C:\benchmark_stephen\data"

# Folder output hasil benchmark
RESULTS_DIR = r"C:\benchmark_stephen\results"
OUTPUT_CSV  = os.path.join(RESULTS_DIR, "duckdb_results.csv")

# ── Parameter benchmark ──────────────────────────────────────
N_RUNS          = 30   # jumlah run terukur per query (override via --n)
DEFAULT_COOLING = 30   # detik cooling antar run (override via --cooling)

# ── Tabel TPC-H ──────────────────────────────────────────────
TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# Estimasi ukuran Parquet per SF
SF_SIZE_MB = {1: 500, 5: 2500, 10: 5000, 20: 10000}

# ══════════════════════════════════════════════════════════════


# ── 15 QUERY TPC-H ───────────────────────────────────────────

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
                p_brand = 'Brand#12'
                AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
                AND l_quantity >= 1 AND l_quantity <= 11
                AND p_size BETWEEN 1 AND 5
                AND l_shipmode IN ('AIR','AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            ) OR (
                p_brand = 'Brand#23'
                AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK')
                AND l_quantity >= 10 AND l_quantity <= 20
                AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR','AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            ) OR (
                p_brand = 'Brand#34'
                AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG')
                AND l_quantity >= 20 AND l_quantity <= 30
                AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR','AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            );
        """,
    },

    # ── JOIN ─────────────────────────────────────────────────
    "Q3_join": {
        "category": "join", "tpch_ref": "Q3",
        "sql": """
            SELECT l_orderkey,
                   SUM(l_extendedprice*(1-l_discount)) AS revenue,
                   o_orderdate, o_shippriority
            FROM customer
            JOIN orders   ON c_custkey  = o_custkey
            JOIN lineitem ON l_orderkey = o_orderkey
            WHERE c_mktsegment = 'BUILDING'
              AND o_orderdate  < DATE '1995-03-15'
              AND l_shipdate   > DATE '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10;
        """,
    },
    "Q5_join": {
        "category": "join", "tpch_ref": "Q5",
        "sql": """
            SELECT n_name, SUM(l_extendedprice*(1-l_discount)) AS revenue
            FROM customer
            JOIN orders   ON c_custkey   = o_custkey
            JOIN lineitem ON l_orderkey  = o_orderkey
            JOIN supplier ON l_suppkey   = s_suppkey
            JOIN nation   ON c_nationkey = n_nationkey
                         AND s_nationkey = n_nationkey
            JOIN region   ON n_regionkey = r_regionkey
            WHERE r_name = 'ASIA'
              AND o_orderdate >= DATE '1994-01-01'
              AND o_orderdate  < DATE '1995-01-01'
            GROUP BY n_name
            ORDER BY revenue DESC;
        """,
    },
    "Q10_join": {
        "category": "join", "tpch_ref": "Q10",
        "sql": """
            SELECT c_custkey, c_name,
                   SUM(l_extendedprice*(1-l_discount)) AS revenue,
                   c_acctbal, n_name, c_address, c_phone, c_comment
            FROM customer
            JOIN orders   ON c_custkey   = o_custkey
            JOIN lineitem ON l_orderkey  = o_orderkey
            JOIN nation   ON c_nationkey = n_nationkey
            WHERE o_orderdate >= DATE '1993-10-01'
              AND o_orderdate  < DATE '1994-01-01'
              AND l_returnflag = 'R'
            GROUP BY c_custkey, c_name, c_acctbal,
                     c_phone, n_name, c_address, c_comment
            ORDER BY revenue DESC
            LIMIT 20;
        """,
    },

    # ── Aggregation ──────────────────────────────────────────
    "Q1_agg": {
        "category": "aggregation", "tpch_ref": "Q1",
        "sql": """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity)                            AS sum_qty,
                   SUM(l_extendedprice)                       AS sum_base_price,
                   SUM(l_extendedprice*(1-l_discount))        AS sum_disc_price,
                   SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge,
                   AVG(l_quantity)    AS avg_qty,
                   AVG(l_extendedprice) AS avg_price,
                   AVG(l_discount)   AS avg_disc,
                   COUNT(*)          AS count_order
            FROM lineitem
            WHERE l_shipdate <= DATE '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus;
        """,
    },
    "Q4_agg": {
        "category": "aggregation", "tpch_ref": "Q4",
        "sql": """
            SELECT o_orderpriority, COUNT(*) AS order_count
            FROM orders
            WHERE o_orderdate >= DATE '1993-07-01'
              AND o_orderdate  < DATE '1993-10-01'
              AND EXISTS (
                  SELECT 1 FROM lineitem
                  WHERE l_orderkey   = o_orderkey
                    AND l_commitdate < l_receiptdate
              )
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority;
        """,
    },
    "Q7_agg": {
        "category": "aggregation", "tpch_ref": "Q7",
        "sql": """
            SELECT supp_nation, cust_nation, l_year,
                   SUM(volume) AS revenue
            FROM (
                SELECT n1.n_name AS supp_nation,
                       n2.n_name AS cust_nation,
                       EXTRACT(YEAR FROM l_shipdate) AS l_year,
                       l_extendedprice*(1-l_discount) AS volume
                FROM supplier
                JOIN lineitem ON s_suppkey  = l_suppkey
                JOIN orders   ON l_orderkey = o_orderkey
                JOIN customer ON o_custkey  = c_custkey
                JOIN nation n1 ON s_nationkey = n1.n_nationkey
                JOIN nation n2 ON c_nationkey = n2.n_nationkey
                WHERE (
                    (n1.n_name = 'FRANCE'  AND n2.n_name = 'GERMANY') OR
                    (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                )
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
            SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
            FROM lineitem
            JOIN part ON p_partkey = l_partkey
            WHERE p_brand     = 'Brand#23'
              AND p_container = 'MED BOX'
              AND l_quantity  < (
                  SELECT 0.2 * AVG(l_quantity)
                  FROM lineitem
                  WHERE l_partkey = p_partkey
              );
        """,
    },
    "Q20_subquery": {
        "category": "subquery", "tpch_ref": "Q20",
        "sql": """
            SELECT s_name, s_address
            FROM supplier
            JOIN nation ON s_nationkey = n_nationkey
            WHERE n_name = 'CANADA'
              AND s_suppkey IN (
                  SELECT ps_suppkey FROM partsupp
                  WHERE ps_partkey IN (
                      SELECT p_partkey FROM part
                      WHERE p_name LIKE 'forest%'
                  )
                  AND ps_availqty > (
                      SELECT 0.5 * SUM(l_quantity)
                      FROM lineitem
                      WHERE l_partkey  = ps_partkey
                        AND l_suppkey  = ps_suppkey
                        AND l_shipdate >= DATE '1994-01-01'
                        AND l_shipdate  < DATE '1995-01-01'
                  )
              )
            ORDER BY s_name;
        """,
    },
    "Q22_subquery": {
        "category": "subquery", "tpch_ref": "Q22",
        "sql": """
            SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
            FROM (
                SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
                       c_acctbal
                FROM customer
                WHERE SUBSTRING(c_phone FROM 1 FOR 2)
                      IN ('13','31','23','29','30','18','17')
                  AND c_acctbal > (
                      SELECT AVG(c_acctbal) FROM customer
                      WHERE c_acctbal > 0.00
                        AND SUBSTRING(c_phone FROM 1 FOR 2)
                            IN ('13','31','23','29','30','18','17')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM orders
                      WHERE o_custkey = c_custkey
                  )
            ) AS custsale
            GROUP BY cntrycode
            ORDER BY cntrycode;
        """,
    },

    # ── Window Function ──────────────────────────────────────
    "Q10_window": {
        "category": "window_function", "tpch_ref": "Q10-W",
        "sql": """
            SELECT c_custkey, c_name, revenue,
                   RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
            FROM (
                SELECT c_custkey, c_name,
                       SUM(l_extendedprice*(1-l_discount)) AS revenue
                FROM customer
                JOIN orders   ON c_custkey  = o_custkey
                JOIN lineitem ON l_orderkey = o_orderkey
                JOIN nation   ON c_nationkey = n_nationkey
                WHERE o_orderdate >= DATE '1993-10-01'
                  AND o_orderdate  < DATE '1994-01-01'
                  AND l_returnflag = 'R'
                GROUP BY c_custkey, c_name
            ) AS sub
            ORDER BY revenue_rank
            LIMIT 20;
        """,
    },
    "Q1_window": {
        "category": "window_function", "tpch_ref": "Q1-W",
        "sql": """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity)      AS sum_qty,
                   SUM(l_extendedprice) AS sum_base_price,
                   COUNT(*)             AS count_order,
                   SUM(SUM(l_quantity)) OVER (
                       ORDER BY l_returnflag, l_linestatus
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS running_qty
            FROM lineitem
            WHERE l_shipdate <= DATE '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus;
        """,
    },
    "Q18_window": {
        "category": "window_function", "tpch_ref": "Q18-W",
        "sql": """
            SELECT c_name, c_custkey, o_orderkey, o_orderdate,
                   o_totalprice, total_quantity,
                   RANK() OVER (
                       PARTITION BY c_custkey
                       ORDER BY total_quantity DESC
                   ) AS qty_rank,
                   ROW_NUMBER() OVER (
                       ORDER BY total_quantity DESC
                   ) AS global_row_num
            FROM (
                SELECT c_name, c_custkey, o_orderkey,
                       o_orderdate, o_totalprice,
                       SUM(l_quantity) AS total_quantity
                FROM customer
                JOIN orders   ON c_custkey  = o_custkey
                JOIN lineitem ON o_orderkey = l_orderkey
                GROUP BY c_name, c_custkey, o_orderkey,
                         o_orderdate, o_totalprice
                HAVING SUM(l_quantity) > 300
            ) AS sub
            ORDER BY total_quantity DESC
            LIMIT 100;
        """,
    },
}


# ══════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════

def setup_logging(results_dir: str, scale_factors: list,
                  existing_log: str = None) -> str:
    """
    Dual logging ke terminal + file.
    - Kalau existing_log diisi (--log-file), append ke file itu.
    - Kalau tidak, buat file baru dengan timestamp.
    """
    log_dir = os.path.join(results_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    if existing_log:
        # Gunakan file log yang sudah ada (--log-file)
        log_path = existing_log
    else:
        # Default: fixed filename, selalu append ke file yang sama
        log_path = os.path.join(log_dir, "log_DuckDB.log")

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


def log_raw(msg: str):
    logging.info(msg)


# ── Deteksi RAM info (Windows) ────────────────────────────────

def get_ram_info() -> str:
    """Coba baca total RAM via ctypes (Windows). Fallback ke psutil."""
    try:
        import psutil
        total_gb = psutil.virtual_memory().total / (1024 ** 3)
        avail_gb = psutil.virtual_memory().available / (1024 ** 3)
        return f"{total_gb:.1f} GB total, {avail_gb:.1f} GB available"
    except ImportError:
        return "psutil not installed — install with: pip install psutil"


def get_memory_mb() -> float:
    """Ambil RSS memory proses saat ini dalam MB."""
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except Exception:
        return 0.0


# ── OS Page Cache Flush (Windows) ────────────────────────────

def flush_os_cache() -> bool:
    """
    Flush OS file cache di Windows.
    Butuh privilege Administrator.
    Return True kalau berhasil, False kalau tidak.
    """
    if platform.system() != "Windows":
        log("[SKIP] flush_os_cache: hanya supported di Windows.")
        return False
    try:
        # SetSystemFileCacheSize dengan flag 0x80000000 → flush cache
        # Butuh SE_INCREASE_QUOTA_NAME privilege
        PROCESS_QUERY_INFORMATION = 0x0400
        SE_PRIVILEGE_ENABLED      = 0x00000002

        kernel32 = ctypes.windll.kernel32
        result   = kernel32.SetSystemFileCacheSize(
            ctypes.c_size_t(0xFFFFFFFF),
            ctypes.c_size_t(0xFFFFFFFF),
            ctypes.c_int(0)
        )
        if result:
            log("OS file cache flush: berhasil ✓")
            return True
        else:
            # Coba cara lain: EmptyWorkingSet
            handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, os.getpid())
            kernel32.EmptyWorkingSet(handle)
            kernel32.CloseHandle(handle)
            log("OS file cache flush: partial (EmptyWorkingSet) — jalankan sebagai Admin untuk flush penuh.")
            return True
    except Exception as e:
        log(f"[WARN] OS cache flush gagal: {e}")
        log("       Jalankan script sebagai Administrator untuk flush cache penuh.")
        return False


# ── CSV helpers ───────────────────────────────────────────────

def load_completed(filepath: str, n_runs: int) -> set:
    """
    Baca CSV yang sudah ada.
    Return set (query_name, sf) yang sudah punya >= n_runs baris valid.
    """
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
    """
    Hitung berapa run sudah ada di CSV untuk query + SF tertentu.
    Dipakai untuk resume dari run ke-N+1 (bukan dari 1 lagi).
    """
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
                log(f"[!] CSV terkunci. Retry {attempt}/5 dalam 3 detik...")
                time.sleep(3)
            else:
                raise


def append_result(filepath, query_name, query_info, sf, result,
                  start_run: int = 1):
    """Tulis hasil ke CSV. start_run agar run_number lanjut dari yang sudah ada."""
    rows = []
    for i, t in enumerate(result["times"], start=start_run):
        rows.append([
            "duckdb_local",
            query_name, query_info["tpch_ref"], query_info["category"],
            sf, i,
            round(t, 6),
            round(result["mean"], 6), round(result["std"], 6),
            round(result["min"], 6),  round(result["max"], 6),
            round(result["mem_before"], 2),
            round(result["mem_after"],  2),
            round(result["mem_delta"],  2),
            0.0,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ])
    safe_append_csv(filepath, rows)


# ══════════════════════════════════════════════════════════════
#  VALIDASI DATA
# ══════════════════════════════════════════════════════════════

def validate_sf_data(sf: int, sf_dir_override: str = None) -> tuple[bool, str]:
    """
    Cek semua file Parquet yang dibutuhkan ada dan tidak kosong.
    Return (ok: bool, base_dir: str)
    """
    base_dir = sf_dir_override if sf_dir_override else DATA_DIR
    sf_dir   = os.path.join(base_dir, f"sf{sf}")

    if not os.path.exists(sf_dir):
        log(f"[ERROR] Folder tidak ditemukan: {sf_dir}")
        return False, sf_dir

    missing = []
    for table in TPCH_TABLES:
        path = os.path.join(sf_dir, f"{table}.parquet")
        if not os.path.exists(path):
            missing.append(table)
        elif os.path.getsize(path) == 0:
            missing.append(f"{table} (kosong!)")

    if missing:
        log(f"[ERROR] File Parquet kurang di SF{sf}: {', '.join(missing)}")
        return False, sf_dir

    # Tampilkan ukuran file
    total_mb = 0
    for table in TPCH_TABLES:
        path    = os.path.join(sf_dir, f"{table}.parquet")
        file_mb = os.path.getsize(path) / (1024 * 1024)
        total_mb += file_mb
        log_raw(f"      {table:12s}  {file_mb:7.1f} MB  ✓")
    log_raw(f"      {'TOTAL':12s}  {total_mb:7.1f} MB")

    return True, sf_dir


# ══════════════════════════════════════════════════════════════
#  DUCKDB CONNECTION
# ══════════════════════════════════════════════════════════════

def connect_duckdb(sf: int, sf_dir: str) -> duckdb.DuckDBPyConnection:
    """
    Buat koneksi DuckDB in-memory, buat VIEW ke file Parquet lokal.
    In-memory connection = tidak ada state carry-over antar SF.
    """
    con = duckdb.connect(":memory:")

    # Set threads ke jumlah logical CPU
    import multiprocessing
    n_threads = multiprocessing.cpu_count()
    con.execute(f"PRAGMA threads={n_threads}")
    con.execute("PRAGMA memory_limit='12GB'")  # kasih headroom

    # Buat VIEW untuk setiap tabel
    for table in TPCH_TABLES:
        parquet_path = os.path.join(sf_dir, f"{table}.parquet")
        sql_path     = parquet_path.replace("\\", "/")
        con.execute(
            f"CREATE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{sql_path}')"
        )

    return con


# ══════════════════════════════════════════════════════════════
#  BENCHMARK
# ══════════════════════════════════════════════════════════════

def run_query(con, query_name: str, query_info: dict,
              n_runs: int, cooling: int,
              skip_warmup: bool = False) -> dict:
    """
    1 warm-up (tidak dicatat) + n_runs terukur.
    skip_warmup=True kalau ini resume (engine sudah warm dari run sebelumnya).
    """
    sql = query_info["sql"]

    if not skip_warmup:
        log_raw(f"      Warm-up...")
        t_wu = time.perf_counter()
        con.execute(sql).fetchall()
        log_raw(f"      Warm-up selesai ({time.perf_counter()-t_wu:.3f}s, tidak dicatat)")
    else:
        log_raw(f"      Warm-up dilewati (resume mode)")

    times           = []
    mem_before_list = []
    mem_after_list  = []

    for run_num in range(1, n_runs + 1):
        gc.collect()
        mem_before = get_memory_mb()

        t0      = time.perf_counter()
        result  = con.execute(sql).fetchall()
        elapsed = time.perf_counter() - t0

        mem_after = get_memory_mb()
        times.append(elapsed)
        mem_before_list.append(mem_before)
        mem_after_list.append(mem_after)

        log_raw(
            f"      Run {run_num:02d}/{n_runs}: {elapsed:.4f}s  "
            f"| rows={len(result)}  "
            f"| mem Δ={mem_after-mem_before:+.1f} MB"
        )

        if run_num < n_runs:
            for remaining in range(cooling, 0, -5):
                log_raw(f"      Cooling... {remaining}s tersisa")
                time.sleep(min(5, remaining))

    mean = sum(times) / len(times)
    std  = (sum((t - mean) ** 2 for t in times) / len(times)) ** 0.5

    return {
        "times":      times,
        "mean":       mean,
        "std":        std,
        "min":        min(times),
        "max":        max(times),
        "mem_before": sum(mem_before_list) / len(mem_before_list),
        "mem_after":  sum(mem_after_list)  / len(mem_after_list),
        "mem_delta":  sum(m[1]-m[0] for m in zip(mem_before_list, mem_after_list)) / len(times),
    }


def benchmark_sf(con, sf: int, completed: set,
                 n_runs: int, cooling: int) -> int:
    """
    Jalankan semua 15 query untuk 1 SF.
    Skip query yang sudah selesai di CSV (resume support).
    Kalau query sudah ada sebagian (misal 10 dari 30), lanjutkan dari sisa.
    """
    log(f"Mulai benchmark {len(QUERIES)} query × {n_runs} runs  "
        f"[cooling={cooling}s]...\n")

    done = 0
    for q_num, (query_name, query_info) in enumerate(QUERIES.items(), start=1):
        key = (query_name, sf)

        if key in completed:
            log_raw(
                f"  [{q_num:02d}/{len(QUERIES)}] {query_name} SF{sf} "
                f"— SKIP (sudah {n_runs} runs di CSV)"
            )
            continue

        # Cek berapa run sudah ada (untuk resume parsial, misal 10 → 30)
        existing = count_existing_runs(OUTPUT_CSV, query_name, sf)
        remaining = n_runs - existing

        if existing > 0:
            log_raw(
                f"\n  [{q_num:02d}/{len(QUERIES)}] {query_name} SF{sf} "
                f"— RESUME dari run {existing+1} (sudah {existing}/{n_runs})"
            )
        else:
            log_raw(
                f"\n  [{q_num:02d}/{len(QUERIES)}] {query_name} "
                f"[{query_info['category']}] — {query_info['tpch_ref']}"
            )

        try:
            result = run_query(con, query_name, query_info,
                               remaining, cooling,
                               skip_warmup=(existing > 0))
            append_result(OUTPUT_CSV, query_name, query_info, sf,
                          result, start_run=existing + 1)
            completed.add(key)
            done += 1
            log_raw(
                f"\n      ✓ MEAN={result['mean']:.4f}s  "
                f"STD={result['std']:.4f}s  "
                f"MIN={result['min']:.4f}s  "
                f"MAX={result['max']:.4f}s\n"
            )

        except Exception as e:
            log_raw(f"\n      ✗ ERROR pada {query_name}: {e}")
            log("Mencoba reconnect DuckDB...")
            try:
                con.close()
            except Exception:
                pass

    return done


# ══════════════════════════════════════════════════════════════
#  PROGRESS DISPLAY
# ══════════════════════════════════════════════════════════════

def print_progress(completed: set, scale_factors: list, n_runs: int):
    log_raw(f"\n  {'─'*58}")
    log_raw(f"  PROGRESS  (n={n_runs} per query)")
    log_raw(f"  {'─'*58}")
    for sf in scale_factors:
        sf_done  = sum(1 for qname in QUERIES if (qname, sf) in completed)
        sf_total = len(QUERIES)
        bar_fill = int((sf_done / sf_total) * 24)
        bar      = "█" * bar_fill + "░" * (24 - bar_fill)
        status   = "✓ SELESAI" if sf_done == sf_total else f"{sf_done}/{sf_total}"
        log_raw(f"  SF{sf:2d}  [{bar}]  {status}")
    total_done = sum(
        1 for qname in QUERIES for sf in scale_factors
        if (qname, sf) in completed
    )
    total_all = len(QUERIES) * len(scale_factors)
    log_raw(f"  {'─'*58}")
    log_raw(f"  Total: {total_done}/{total_all} query selesai\n")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="DuckDB Local Benchmark v1 — Stephen Edlin (PRADITA University)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Cara pakai paling simpel:
  python duckdb_local_benchmark_v1.py

Override SF atau cooling:
  python duckdb_local_benchmark_v1.py --sf 1 5
  python duckdb_local_benchmark_v1.py --sf 10 20 --cooling 20
  python duckdb_local_benchmark_v1.py --sf 1 --cooling 30

Kalau terputus, jalankan ulang perintah yang sama — resume otomatis.
        """
    )
    parser.add_argument(
        "--sf", nargs="+", type=int,
        choices=[1, 5, 10, 20],
        default=[1, 5, 10, 20],
        metavar="N",
        help="Scale factor yang diuji. Default: semua (1 5 10 20)."
    )
    parser.add_argument(
        "--cooling", type=int,
        default=DEFAULT_COOLING,
        metavar="SEC",
        help=f"Detik cooling antar run. Default: {DEFAULT_COOLING}s. "
             "Rekomendasi: 30s untuk rigour, 20s kalau mau lebih cepat."
    )
    parser.add_argument(
        "--no-cache-flush", action="store_true",
        help="Skip flush OS page cache antar SF (default: flush)."
    )
    parser.add_argument(
        "--sf-dir", type=str, default=None,
        metavar="PATH",
        help=f"Override folder data TPC-H. Default: {DATA_DIR}"
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
             "Contoh: --log-file \"C:\\benchmark_stephen\\results\\logs\\log_DuckDB.log\""
    )
    return parser.parse_args()


if __name__ == "__main__":

    args          = parse_args()
    scale_factors = sorted(set(args.sf))
    cooling       = args.cooling
    do_flush      = not args.no_cache_flush
    sf_dir_root   = args.sf_dir
    existing_log  = args.log_file

    # Resolve n_runs: --n override N_RUNS, tapi tidak boleh kurang dari yang sudah ada di CSV
    n_runs = args.n if args.n is not None else N_RUNS
    if n_runs < 1:
        print(f"[ERROR] --n harus >= 1 (dapat: {n_runs})")
        sys.exit(1)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    log_path = setup_logging(RESULTS_DIR, scale_factors, existing_log)

    banner("DuckDB Local Benchmark v2 — Stephen Edlin (PRADITA University)")
    logging.info(f"  Platform   : {platform.system()} {platform.version()[:40]}")
    logging.info(f"  Python     : {sys.version.split()[0]}")
    logging.info(f"  DuckDB     : {duckdb.__version__}")
    logging.info(f"  RAM        : {get_ram_info()}")
    logging.info(f"  Data dir   : {sf_dir_root or DATA_DIR}")
    logging.info(f"  Output CSV : {OUTPUT_CSV}")
    logging.info(f"  Log file   : {log_path} (APPEND)")
    logging.info(f"  Scale Factors : {scale_factors}")
    logging.info(f"  N runs     : {n_runs} (target per query per SF)")
    logging.info(f"  Cooling    : {cooling}s antar run")
    logging.info(f"  Cache flush: {'YA antar SF' if do_flush else 'TIDAK (--no-cache-flush)'}")
    logging.info("")
    logging.info("  Catatan metodologi:")
    logging.info("    • Setiap SF pakai in-memory DuckDB connection baru")
    logging.info(f"    • 1 warm-up run sebelum measured runs (skip kalau resume)")
    logging.info(f"    • Cooling {cooling}s antar run (CPU thermal settle)")
    logging.info("    • Memory RSS dicatat tiap run via psutil")
    logging.info(f"    • Resume otomatis: hitung run yang sudah ada, lanjut dari sisa")

    # ── Cek psutil tersedia ──────────────────────────────────
    try:
        import psutil
    except ImportError:
        logging.info("\n  [WARN] psutil tidak ditemukan.")
        logging.info("  Install dulu: pip install psutil")
        logging.info("  Memory tracking akan dinonaktifkan.")

    # ── Setup CSV ────────────────────────────────────────────
    write_csv_header(OUTPUT_CSV)
    completed = load_completed(OUTPUT_CSV, n_runs)

    if completed:
        already = sum(
            1 for qname in QUERIES for sf in scale_factors
            if (qname, sf) in completed
        )
        logging.info(f"\n  Resume mode: {already} query untuk SF{scale_factors} "
                     f"sudah lengkap {n_runs} runs di CSV → akan di-skip.")
        logging.info(f"  Query yang belum lengkap akan dilanjutkan dari run yang tersisa.")

    print_progress(completed, scale_factors, n_runs)

    grand_start = time.perf_counter()
    grand_done  = 0

    # ── Loop per SF ──────────────────────────────────────────
    for sf in scale_factors:

        if sf_fully_done(completed, sf):
            log(f"SF{sf} sudah lengkap ({len(QUERIES)} query × {n_runs} runs) → skip.")
            continue

        banner(
            f"SCALE FACTOR {sf}  (~{SF_SIZE_MB.get(sf,0)//1024} GB)",
            char="-"
        )

        # ── Validasi data ────────────────────────────────────
        log(f"Validasi file Parquet SF{sf}...")
        ok, sf_dir = validate_sf_data(sf, sf_dir_root)
        if not ok:
            log(f"[ERROR] Data SF{sf} tidak valid → lewati SF ini.")
            continue
        logging.info("")

        # ── Flush cache sebelum SF baru ──────────────────────
        if do_flush:
            log("Flush OS page cache...")
            flush_os_cache()

        gc.collect()

        # ── Buka koneksi DuckDB baru (fresh per SF) ──────────
        log(f"Membuka koneksi DuckDB in-memory untuk SF{sf}...")
        try:
            con = connect_duckdb(sf, sf_dir)
            log("Koneksi DuckDB OK ✓")
        except Exception as e:
            log(f"[ERROR] Gagal buka DuckDB: {e}")
            continue

        # ── Benchmark ────────────────────────────────────────
        done_count = benchmark_sf(con, sf, completed, n_runs, cooling)
        grand_done += done_count

        try:
            con.close()
        except Exception:
            pass

        if sf_fully_done(completed, sf):
            log(f"SF{sf}: Semua {len(QUERIES)} query selesai ({n_runs} runs) ✓")
        else:
            missing = sum(
                1 for qname in QUERIES
                if (qname, sf) not in completed
            )
            log(f"[WARN] SF{sf}: {missing} query belum selesai.")
            log("       Jalankan ulang perintah yang sama untuk resume.")

        print_progress(completed, scale_factors, n_runs)

        # ── Jeda antar SF ────────────────────────────────────
        idx = scale_factors.index(sf)
        if idx < len(scale_factors) - 1:
            next_sf = scale_factors[idx + 1]
            if not sf_fully_done(completed, next_sf):
                log(f"Jeda 60 detik sebelum lanjut ke SF{next_sf} "
                    f"(biarkan sistem settle)...")
                time.sleep(60)

    # ── Selesai ──────────────────────────────────────────────
    grand_elapsed = time.perf_counter() - grand_start

    banner("BENCHMARK SELESAI!")
    logging.info(f"  Query baru diselesaikan : {grand_done}")
    logging.info(
        f"  Total waktu             : "
        f"{grand_elapsed/3600:.2f} jam ({grand_elapsed/60:.1f} menit)"
    )
    logging.info(f"  Output CSV              : {OUTPUT_CSV}")
    logging.info(f"  Log file                : {log_path}")
    logging.info(f"  Selesai                 : "
                 f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("")
    logging.info("  File siap untuk analisis:")
    logging.info("    duckdb_results.csv        ← data lokal (file ini)")
    logging.info("    motherduck_results.csv    ← data cloud (dari v4)")
    logging.info("")
    logging.info("  Gabungkan kedua CSV untuk analisis komparatif.")
    logging.info(f"  Log tersimpan di : {log_path}")
    logging.info("=" * 62 + "\n")
