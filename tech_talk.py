import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell
def imports():
    import marimo as mo
    import duckdb
    import os
    import shutil
    return duckdb, mo, os, shutil


@app.cell
def title(mo):
    return mo.md(r"""
    # Delta Lake: A Lakehouse Format

    **Building reliable, ACID-compliant data lakes with open table formats**

    > *We'll demo these concepts using DuckLake locally — same principles, zero infrastructure.*
    """)


@app.cell
def background(mo):
    return mo.md(r"""
    ## Background: The Data Lake Problem

    The modern data stack settled on two good ideas:

    1. **Separate storage from compute** — store data in cheap object storage (S3, ADLS, GCS),
       run compute separately. Both scale independently.
    2. **Use open formats** — store data as Parquet files. No vendor lock-in.
       Any engine can read it.

    The result: a *data lake* — petabytes of Parquet files on blob storage.

    **The problem:** you can append files easily, but *changing* data requires
    custom scripts with no correctness guarantees. No transactions. No ACID.
    """)


@app.cell
def delta_iceberg(mo):
    return mo.md(r"""
    ## Enter Delta Lake (and Apache Iceberg)

    Delta Lake and Apache Iceberg solve this by adding a **transaction log** on top
    of the Parquet files — without giving up object storage or open formats.

    **Delta Lake's approach:**
    - A `_delta_log/` folder alongside your data files
    - Each commit writes a new JSON file (e.g., `000000000000000000001.json`)
    - The log records what files were added/removed per transaction
    - ACID via **optimistic concurrency**: atomic `put-if-absent` on the log file

    ![Iceberg table architecture](https://ducklake.select/images/manifesto/iceberg-table-architecture.png)
    *Iceberg uses the same idea: metadata files + manifest lists + manifest files + Parquet*

    **What you get:** time travel, schema enforcement, upserts (MERGE INTO),
    concurrent writes, snapshot isolation. A full lakehouse.
    """)


@app.cell
def catalog_problem(mo):
    return mo.md(r"""
    ## The Hidden Problem: You Need a Database Anyway

    File-based transaction logs hit real limits at scale:

    | Problem | Cause |
    |---------|-------|
    | Slow metadata queries | Must list/read thousands of small JSON files |
    | Small file explosion | Every small write = new Parquet + new log file |
    | Concurrent write contention | Optimistic retries under high write load |
    | Catalog needed for discovery | What tables exist? Which S3 prefix? |

    The solution the ecosystem converged on: **add a catalog service** backed by a database.
    The catalog stores a pointer to the current table version. Consistency borrowed from the DB.

    ![Iceberg catalog architecture](https://ducklake.select/images/manifesto/iceberg-catalog-architecture.png)

    **The irony:** both formats were designed to *avoid* needing a database.
    They ended up needing one anyway — just for a tiny pointer table.
    """)


@app.cell
def ducklake_intro(mo):
    return mo.md(r"""
    ## DuckLake: Go All-In on SQL

    If you need a database for the catalog anyway, why not use it for *all* metadata?

    DuckLake's design: **move every metadata structure into a SQL database**.
    The Parquet data files stay on blob storage. Everything else — schemas, snapshots,
    file lists, statistics, column stats — lives in a SQL catalog (DuckDB, PostgreSQL, SQLite).

    ![DuckLake architecture](https://ducklake.select/images/manifesto/ducklake-architecture.png)

    A single SQL transaction records a commit:
    ```sql
    BEGIN;
      INSERT INTO ducklake_data_file VALUES (..., 'path/to/file.parquet', ...);
      INSERT INTO ducklake_table_stats VALUES (...);
      INSERT INTO ducklake_snapshot VALUES (...);
    COMMIT;
    ```

    ![DuckLake schema](https://ducklake.select/images/manifesto/ducklake-schema-1.png)

    **This is what BigQuery (Spanner) and Snowflake (FoundationDB) do** — just without
    the open formats at the bottom.
    """)


@app.cell
def comparison(mo):
    return mo.md(r"""
    ## Delta Lake vs DuckLake

    | | Delta Lake | DuckLake |
    |---|---|---|
    | **Metadata store** | JSON files in `_delta_log/` | SQL database (DuckDB / Postgres / SQLite) |
    | **ACID mechanism** | Optimistic concurrency on file writes | Database transactions (native MVCC) |
    | **Small writes** | Creates many small files, needs compaction | Optionally inlines data into catalog DB |
    | **Metadata queries** | List + read many files (slow at scale) | Single SQL query (fast) |
    | **Scale target** | Distributed, cloud-native, PB-scale | Local → distributed via catalog DB |
    | **Ecosystem** | Spark, Databricks, Flink, Trino | DuckDB-centric (multi-engine on roadmap) |
    | **License** | Apache 2.0 (Linux Foundation) | MIT (DuckDB Foundation) |
    | **Production since** | 2019 | v1.0 April 2026 |

    > **For this demo:** We use DuckLake locally because it requires zero infrastructure.
    > Every concept below maps 1:1 to Delta Lake in production.
    """)


@app.cell
def demo_setup(duckdb, mo, os, shutil):
    # Idempotent: wipe any previous run
    catalog = "demo.ducklake"
    files_dir = "demo.ducklake.files"
    if os.path.exists(catalog):
        os.remove(catalog)
    if os.path.exists(files_dir):
        shutil.rmtree(files_dir)

    conn = duckdb.connect()
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    conn.execute(f"ATTACH 'ducklake:{catalog}' AS lake")
    conn.execute("USE lake")
    conn.execute("""
        CREATE TABLE products (
            id      INTEGER,
            name    VARCHAR,
            price   DECIMAL(10, 2)
        )
    """)

    mo.md(r"""
    ## Demo Setup ✓

    A fresh local DuckLake is attached as `lake`.
    Metadata lives in `demo.ducklake` (a DuckDB file).
    Parquet data files will appear in `demo.ducklake.files/`.

    Run cells below **in order** to walk through each concept.
    """)
    return (conn,)


if __name__ == "__main__":
    app.run()
