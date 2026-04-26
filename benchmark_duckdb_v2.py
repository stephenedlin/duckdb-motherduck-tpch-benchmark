"""
=============================================================
 Script 2 — benchmark_duckdb.py  (v2 — resume friendly)
 Penelitian: Edge Analytics vs Cloud Analytics
 Peneliti  : Stephen Edlin — PRADITA University
=============================================================
 Perubahan dari v1:
 - Auto-detect query yang sudah selesai di CSV → skip otomatis
 - Jangan buka CSV di Excel saat script jalan!
 - Kalau mau lihat progress, buka CSV dengan Notepad saja
=============================================================
"""

import duckdb
import time
import os
import csv
import psutil
import gc
from datetime import datetime

# ── KONFIGURASI ──────────────────────────────────────────────
DATA_DIR    = r"C:\benchmark_stephen\data"
RESULTS_DIR = r"C:\benchmark_stephen\results"
OUTPUT_CSV  = os.path.join(RESULTS_DIR, "duckdb_results.csv")

SCALE_FACTORS  = [1, 5, 10, 20]
N_RUNS         = 5
COOLING_SEC    = 30
# ─────────────────────────────────────────────────────────────


# ── 15 QUERY TPC-H (5 kategori × 3 query) ───────────────────
QUERIES = {

    # ── KATEGORI 1: Simple SELECT ──────────────────────────
    "Q6_simple": {
        "category": "simple_select",
        "tpch_ref": "Q6",
        "sql": """
            SELECT
                SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE
                l_shipdate >= DATE '1994-01-01'
                AND l_shipdate < DATE '1995-01-01'
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24;
        """,
    },
    "Q14_simple": {
        "category": "simple_select",
        "tpch_ref": "Q14",
        "sql": """
            SELECT
                100.00 * SUM(CASE
                    WHEN p_type LIKE 'PROMO%'
                    THEN l_extendedprice * (1 - l_discount)
                    ELSE 0
                END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM lineitem
            JOIN part ON l_partkey = p_partkey
            WHERE
                l_shipdate >= DATE '1995-09-01'
                AND l_shipdate < DATE '1995-10-01';
        """,
    },
    "Q19_simple": {
        "category": "simple_select",
        "tpch_ref": "Q19",
        "sql": """
            SELECT
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM lineitem
            JOIN part ON p_partkey = l_partkey
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

    # ── KATEGORI 2: JOIN Operations ────────────────────────
    "Q3_join": {
        "category": "join",
        "tpch_ref": "Q3",
        "sql": """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM customer
            JOIN orders   ON c_custkey = o_custkey
            JOIN lineitem ON l_orderkey = o_orderkey
            WHERE
                c_mktsegment = 'BUILDING'
                AND o_orderdate < DATE '1995-03-15'
                AND l_shipdate  > DATE '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10;
        """,
    },
    "Q5_join": {
        "category": "join",
        "tpch_ref": "Q5",
        "sql": """
            SELECT
                n_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM customer
            JOIN orders   ON c_custkey   = o_custkey
            JOIN lineitem ON l_orderkey  = o_orderkey
            JOIN supplier ON l_suppkey   = s_suppkey
            JOIN nation   ON c_nationkey = n_nationkey
                          AND s_nationkey = n_nationkey
            JOIN region   ON n_regionkey = r_regionkey
            WHERE
                r_name = 'ASIA'
                AND o_orderdate >= DATE '1994-01-01'
                AND o_orderdate <  DATE '1995-01-01'
            GROUP BY n_name
            ORDER BY revenue DESC;
        """,
    },
    "Q10_join": {
        "category": "join",
        "tpch_ref": "Q10",
        "sql": """
            SELECT
                c_custkey,
                c_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            FROM customer
            JOIN orders   ON c_custkey  = o_custkey
            JOIN lineitem ON l_orderkey = o_orderkey
            JOIN nation   ON c_nationkey = n_nationkey
            WHERE
                o_orderdate >= DATE '1993-10-01'
                AND o_orderdate < DATE '1994-01-01'
                AND l_returnflag = 'R'
            GROUP BY
                c_custkey, c_name, c_acctbal,
                c_phone, n_name, c_address, c_comment
            ORDER BY revenue DESC
            LIMIT 20;
        """,
    },

    # ── KATEGORI 3: Aggregation ────────────────────────────
    "Q1_agg": {
        "category": "aggregation",
        "tpch_ref": "Q1",
        "sql": """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity)                                        AS sum_qty,
                SUM(l_extendedprice)                                   AS sum_base_price,
                SUM(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                AVG(l_quantity)                                        AS avg_qty,
                AVG(l_extendedprice)                                   AS avg_price,
                AVG(l_discount)                                        AS avg_disc,
                COUNT(*)                                               AS count_order
            FROM lineitem
            WHERE l_shipdate <= DATE '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus;
        """,
    },
    "Q4_agg": {
        "category": "aggregation",
        "tpch_ref": "Q4",
        "sql": """
            SELECT
                o_orderpriority,
                COUNT(*) AS order_count
            FROM orders
            WHERE
                o_orderdate >= DATE '1993-07-01'
                AND o_orderdate < DATE '1993-10-01'
                AND EXISTS (
                    SELECT 1 FROM lineitem
                    WHERE l_orderkey = o_orderkey
                    AND l_commitdate < l_receiptdate
                )
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority;
        """,
    },
    "Q7_agg": {
        "category": "aggregation",
        "tpch_ref": "Q7",
        "sql": """
            SELECT
                supp_nation,
                cust_nation,
                l_year,
                SUM(volume) AS revenue
            FROM (
                SELECT
                    n1.n_name AS supp_nation,
                    n2.n_name AS cust_nation,
                    EXTRACT(YEAR FROM l_shipdate) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume
                FROM supplier
                JOIN lineitem ON s_suppkey   = l_suppkey
                JOIN orders   ON o_orderkey  = l_orderkey
                JOIN customer ON c_custkey   = o_custkey
                JOIN nation n1 ON s_nationkey = n1.n_nationkey
                JOIN nation n2 ON c_nationkey = n2.n_nationkey
                WHERE (
                    (n1.n_name = 'FRANCE'  AND n2.n_name = 'GERMANY')
                    OR
                    (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                )
                AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            ) AS shipping
            GROUP BY supp_nation, cust_nation, l_year
            ORDER BY supp_nation, cust_nation, l_year;
        """,
    },

    # ── KATEGORI 4: Subqueries ─────────────────────────────
    "Q17_sub": {
        "category": "subquery",
        "tpch_ref": "Q17",
        "sql": """
            SELECT
                SUM(l_extendedprice) / 7.0 AS avg_yearly
            FROM lineitem
            JOIN part ON p_partkey = l_partkey
            WHERE
                p_brand     = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < (
                    SELECT 0.2 * AVG(l_quantity)
                    FROM lineitem
                    WHERE l_partkey = p_partkey
                );
        """,
    },
    "Q20_sub": {
        "category": "subquery",
        "tpch_ref": "Q20",
        "sql": """
            SELECT
                s_name,
                s_address
            FROM supplier
            JOIN nation ON s_nationkey = n_nationkey
            WHERE
                n_name = 'CANADA'
                AND s_suppkey IN (
                    SELECT ps_suppkey
                    FROM partsupp
                    WHERE ps_partkey IN (
                        SELECT p_partkey
                        FROM part
                        WHERE p_name LIKE 'forest%'
                    )
                    AND ps_availqty > (
                        SELECT 0.5 * SUM(l_quantity)
                        FROM lineitem
                        WHERE l_partkey  = ps_partkey
                        AND l_suppkey  = ps_suppkey
                        AND l_shipdate >= DATE '1994-01-01'
                        AND l_shipdate <  DATE '1995-01-01'
                    )
                )
            ORDER BY s_name;
        """,
    },
    "Q22_sub": {
        "category": "subquery",
        "tpch_ref": "Q22",
        "sql": """
            SELECT
                cntrycode,
                COUNT(*)       AS numcust,
                SUM(c_acctbal) AS totacctbal
            FROM (
                SELECT
                    SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
                    c_acctbal
                FROM customer
                WHERE
                    SUBSTRING(c_phone FROM 1 FOR 2) IN
                        ('13','31','23','29','30','18','17')
                    AND c_acctbal > (
                        SELECT AVG(c_acctbal)
                        FROM customer
                        WHERE c_acctbal > 0.00
                        AND SUBSTRING(c_phone FROM 1 FOR 2) IN
                            ('13','31','23','29','30','18','17')
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

    # ── KATEGORI 5: Window Functions (adapted) ─────────────
    "Q10_window": {
        "category": "window_function",
        "tpch_ref": "Q10_adapted",
        "sql": """
            SELECT
                c_custkey,
                c_name,
                revenue,
                n_name,
                RANK() OVER (
                    PARTITION BY n_name
                    ORDER BY revenue DESC
                ) AS rank_in_nation
            FROM (
                SELECT
                    c_custkey,
                    c_name,
                    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                    n_name
                FROM customer
                JOIN orders   ON c_custkey  = o_custkey
                JOIN lineitem ON l_orderkey = o_orderkey
                JOIN nation   ON c_nationkey = n_nationkey
                WHERE
                    o_orderdate >= DATE '1993-10-01'
                    AND o_orderdate < DATE '1994-01-01'
                    AND l_returnflag = 'R'
                GROUP BY c_custkey, c_name, n_name
            ) sub
            ORDER BY n_name, rank_in_nation
            LIMIT 50;
        """,
    },
    "Q18_window": {
        "category": "window_function",
        "tpch_ref": "Q18_adapted",
        "sql": """
            SELECT
                c_name,
                o_orderkey,
                o_orderdate,
                o_totalprice,
                total_qty,
                ROW_NUMBER() OVER (
                    PARTITION BY c_name
                    ORDER BY total_qty DESC
                ) AS row_num,
                SUM(o_totalprice) OVER (
                    PARTITION BY c_name
                    ORDER BY o_orderdate
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_revenue
            FROM (
                SELECT
                    c_name,
                    o_orderkey,
                    o_orderdate,
                    o_totalprice,
                    SUM(l_quantity) AS total_qty
                FROM customer
                JOIN orders   ON c_custkey  = o_custkey
                JOIN lineitem ON o_orderkey = l_orderkey
                GROUP BY c_name, o_orderkey, o_orderdate, o_totalprice
                HAVING SUM(l_quantity) > 300
            ) sub
            ORDER BY c_name, row_num
            LIMIT 50;
        """,
    },
    "Q1_window": {
        "category": "window_function",
        "tpch_ref": "Q1_adapted",
        "sql": """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                AVG(l_discount)                          AS avg_discount,
                COUNT(*)                                 AS line_count,
                SUM(SUM(l_extendedprice * (1 - l_discount))) OVER (
                    ORDER BY l_returnflag, l_linestatus
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_revenue
            FROM lineitem
            WHERE l_shipdate <= DATE '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus;
        """,
    },
}
# ─────────────────────────────────────────────────────────────


def get_memory_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def load_completed(filepath: str) -> set:
    """
    Baca CSV yang sudah ada, return set (query_name, scale_factor)
    yang sudah punya 5 run lengkap. Ini untuk skip query yang sudah selesai.
    """
    completed = set()
    if not os.path.exists(filepath):
        return completed

    counts = {}
    with open(filepath, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip baris error
            if row.get("run_number", "") == "ERROR":
                continue
            key = (row["query_name"], int(row["scale_factor"]))
            counts[key] = counts.get(key, 0) + 1

    # Tandai sebagai completed kalau sudah ada 5 run
    for key, count in counts.items():
        if count >= N_RUNS:
            completed.add(key)

    return completed


def run_single_query(con, query_name: str, query_info: dict) -> dict:
    sql = query_info["sql"]

    # Warm-up run (tidak dicatat)
    print("      Warm-up...", end=" ", flush=True)
    con.execute(sql).fetchall()
    print("selesai")

    times = []
    mem_before = get_memory_mb()

    for run_num in range(1, N_RUNS + 1):
        t_start = time.perf_counter()
        result  = con.execute(sql).fetchall()
        t_end   = time.perf_counter()

        elapsed = t_end - t_start
        times.append(elapsed)

        print(f"      Run {run_num}/{N_RUNS}: {elapsed:.4f}s  "
              f"({len(result)} rows)")

        if run_num < N_RUNS:
            print(f"      Cooling {COOLING_SEC}s...", end=" ", flush=True)
            time.sleep(COOLING_SEC)
            print("lanjut")

    mem_after = get_memory_mb()
    mean = sum(times) / len(times)

    return {
        "times"        : times,
        "mean_time"    : mean,
        "min_time"     : min(times),
        "max_time"     : max(times),
        "std_time"     : (sum((t - mean)**2 for t in times) / len(times)) ** 0.5,
        "memory_before": mem_before,
        "memory_after" : mem_after,
        "memory_delta" : mem_after - mem_before,
    }


def load_tables(con, sf: int):
    sf_dir = os.path.join(DATA_DIR, f"sf{sf}")
    for table in ["lineitem","orders","customer","supplier",
                  "part","partsupp","nation","region"]:
        path = os.path.join(sf_dir, f"{table}.parquet")
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{path}');"
        )


def write_csv_header(filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not os.path.exists(filepath):
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "platform","query_name","tpch_ref","category",
                "scale_factor","run_number","exec_time_s",
                "mean_time_s","std_time_s","min_time_s","max_time_s",
                "memory_before_mb","memory_after_mb","memory_delta_mb",
                "timestamp",
            ])


def safe_append_csv(filepath: str, rows: list):
    """
    Tulis ke CSV dengan retry otomatis kalau file sedang dibuka Excel.
    Coba maksimal 5 kali dengan jeda 3 detik.
    """
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            with open(filepath, "a", newline="") as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow(row)
            return  # sukses
        except PermissionError:
            if attempt < max_retries:
                print(f"\n      [!] CSV sedang dibuka aplikasi lain. "
                      f"Tutup Excel/Notepad dulu! "
                      f"Retry {attempt}/{max_retries} dalam 3 detik...")
                time.sleep(3)
            else:
                print(f"\n      [!] Gagal menulis ke CSV setelah "
                      f"{max_retries} percobaan.")
                print(f"      Data query ini mungkin tidak tersimpan.")
                raise


def append_csv_rows(filepath: str, query_name: str, query_info: dict,
                    sf: int, result: dict):
    rows = []
    for i, t in enumerate(result["times"], start=1):
        rows.append([
            "duckdb",
            query_name,
            query_info["tpch_ref"],
            query_info["category"],
            sf,
            i,
            round(t,                       6),
            round(result["mean_time"],     6),
            round(result["std_time"],      6),
            round(result["min_time"],      6),
            round(result["max_time"],      6),
            round(result["memory_before"], 2),
            round(result["memory_after"],  2),
            round(result["memory_delta"],  2),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ])
    safe_append_csv(filepath, rows)


# ── MAIN ─────────────────────────────────────────────────────
if __name__ == "__main__":

    print("\n" + "="*60)
    print("  DuckDB Benchmark — TPC-H  (v2 resume-friendly)")
    print("  Penelitian: Edge Analytics vs Cloud Analytics")
    print("  Peneliti  : Stephen Edlin — PRADITA University")
    print("="*60)
    print(f"  Output CSV: {OUTPUT_CSV}")
    print()
    print("  PENTING: Jangan buka CSV di Excel saat script jalan!")
    print("  Kalau mau lihat progress: buka dengan Notepad saja.")
    print("="*60)

    write_csv_header(OUTPUT_CSV)

    # Load daftar query yang sudah selesai dari CSV
    completed = load_completed(OUTPUT_CSV)
    if completed:
        print(f"\n  Ditemukan {len(completed)} kombinasi query+SF yang sudah selesai.")
        print("  Query tersebut akan di-skip otomatis (resume mode).\n")
    else:
        print("\n  Mulai dari awal.\n")

    grand_start = time.perf_counter()
    total_done  = 0
    total_skip  = 0

    for sf in SCALE_FACTORS:
        print(f"\n{'='*60}")
        print(f"  SCALE FACTOR {sf} (~{sf} GB)")
        print(f"{'='*60}")

        # Cek apakah semua query di SF ini sudah selesai
        sf_all_done = all(
            (qname, sf) in completed for qname in QUERIES
        )
        if sf_all_done:
            print(f"  SF{sf} sudah lengkap, skip.\n")
            total_skip += len(QUERIES)
            continue

        gc.collect()
        con = duckdb.connect()
        con.execute("SET threads = 12;")

        print(f"  Loading tabel dari data/sf{sf}/...")
        load_tables(con, sf)
        print(f"  Tabel siap.\n")

        for q_num, (query_name, query_info) in enumerate(QUERIES.items(), start=1):

            key = (query_name, sf)

            # Skip kalau sudah selesai
            if key in completed:
                print(f"  [{q_num:02d}/{len(QUERIES)}] {query_name} "
                      f"SF{sf} — SKIP (sudah ada di CSV)")
                total_skip += 1
                continue

            print(f"  [{q_num:02d}/{len(QUERIES)}] {query_name} "
                  f"({query_info['category']}) — TPC-H {query_info['tpch_ref']}")

            try:
                result = run_single_query(con, query_name, query_info)
                append_csv_rows(OUTPUT_CSV, query_name, query_info, sf, result)

                print(f"      MEAN={result['mean_time']:.4f}s  "
                      f"STD={result['std_time']:.4f}s  "
                      f"MEM delta={result['memory_delta']:+.1f}MB")

                total_done += 1
                completed.add(key)  # update in-memory tracker

            except Exception as e:
                print(f"      ERROR: {e}")

            print()

        con.close()
        print(f"  SF{sf} selesai!\n")

    grand_elapsed = time.perf_counter() - grand_start

    print("\n" + "="*60)
    print("  BENCHMARK DUCKDB SELESAI!")
    print("="*60)
    print(f"  Query selesai  : {total_done}")
    print(f"  Query di-skip  : {total_skip}")
    print(f"  Total waktu    : {grand_elapsed/60:.1f} menit")
    print(f"  Hasil CSV      : {OUTPUT_CSV}")
    print("\n  Lanjut ke benchmark_bigquery.py!")
    print("="*60 + "\n")
