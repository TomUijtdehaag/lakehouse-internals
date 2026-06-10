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


@app.cell
def demo_insert(conn, mo):
    conn.execute("""
        INSERT INTO products VALUES
            (1, 'Gouda',       3.49),
            (2, 'Stroopwafel', 2.19),
            (3, 'Hagelslag',   1.89)
    """)

    data = conn.execute("FROM products").df()
    files = conn.execute("FROM glob('demo.ducklake.files/**/*.parquet')").df()

    mo.vstack([
        mo.md(r"""
        ## Concept: Immutable Parquet Files

        After an INSERT, DuckLake writes a new **immutable Parquet file** to storage
        and records it in the SQL catalog with a new snapshot.

        > **In Delta Lake:** same thing — a Parquet file lands in your S3 prefix,
        > and `_delta_log/00...01.json` records the `add` action.
        """),
        mo.md("**Table contents:**"), data,
        mo.md("**Parquet files on disk:**"), files,
    ])
    return (conn, data)


@app.cell
def demo_delete(conn, data, mo):
    conn.execute("DELETE FROM products WHERE id = 2")

    after_delete = conn.execute("FROM products").df()
    files = conn.execute("FROM glob('demo.ducklake.files/**/*.parquet')").df()

    mo.vstack([
        mo.md(r"""
        ## Concept: Deletion Files (No Overwrite)

        Deletes **never modify** the original Parquet file.
        Instead, a new `-delete.parquet` file is written that marks which rows are gone.
        The original data file is untouched — this is what enables time travel.

        > **In Delta Lake:** same pattern — a `remove` action in the log plus
        > a deletion vector (or a separate delete file in older versions).
        """),
        mo.md("**Table after delete:**"), after_delete,
        mo.md("**Files on disk (note the `-delete` file):**"), files,
    ])
    return (conn, after_delete)


@app.cell
def demo_snapshots(conn, after_delete, mo):
    snapshots = conn.execute("FROM ducklake_snapshots('lake')").df()

    mo.vstack([
        mo.md(r"""
        ## Concept: The Snapshot Log

        Every write (INSERT, DELETE, UPDATE, schema change) creates a new **snapshot**.
        The snapshot log is the source of truth for what the table looked like at any point.

        > **In Delta Lake:** each snapshot corresponds to one JSON file in `_delta_log/`.
        > In DuckLake, it's rows in the `ducklake_snapshot` SQL table — much cheaper to query.
        """),
        snapshots,
    ])
    return (conn, snapshots)


@app.cell
def demo_time_travel(conn, snapshots, mo):
    # Snapshot 2 = after INSERT (before DELETE)
    past_state = conn.execute("FROM products AT (VERSION => 2)").df()
    current_state = conn.execute("FROM products").df()

    mo.vstack([
        mo.md(r"""
        ## Concept: Time Travel

        Query any table **as it was at a previous snapshot**.
        This works because the original Parquet files are never deleted — only logically hidden.

        > **In Delta Lake:** `SELECT * FROM table VERSION AS OF 2`
        > In DuckLake: `FROM table AT (VERSION => 2)` — identical concept.
        """),
        mo.md("**Past state (version 2 — before the delete):**"), past_state,
        mo.md("**Current state:**"), current_state,
    ])
    return (conn, past_state)


@app.cell
def demo_changes(conn, past_state, mo):
    changes = conn.execute(
        "FROM ducklake_table_changes('lake', 'main', 'products', 2, 3)"
    ).df()

    mo.vstack([
        mo.md(r"""
        ## Concept: Change Data Feed

        Retrieve exactly what changed between two snapshots — inserts, updates, deletes.
        Useful for incremental processing pipelines.

        > **In Delta Lake:** Change Data Feed (CDF) — enable with
        > `ALTER TABLE SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`,
        > then query with `table_changes('table', startVersion, endVersion)`.
        """),
        changes,
    ])
    return (conn, changes)


@app.cell
def demo_rollback(conn, changes, mo):
    conn.execute("BEGIN TRANSACTION")
    conn.execute("DELETE FROM products")
    mid_tx = conn.execute("FROM products").df()
    conn.execute("ROLLBACK")
    after_rollback = conn.execute("FROM products").df()

    mo.vstack([
        mo.md(r"""
        ## Concept: ACID Transactions

        Wrap multiple operations in a transaction. If something goes wrong, `ROLLBACK`
        undoes everything atomically — no partial writes in the lake.

        > **In Delta Lake:** multi-statement transactions are supported within a single
        > engine session. Cross-engine transactions require a catalog with locking support.
        > In DuckLake, the catalog DB handles locking natively.
        """),
        mo.md("**During transaction (after DELETE, before COMMIT):**"), mid_tx,
        mo.md("**After ROLLBACK — data is back:**"), after_rollback,
    ])
    return (conn, after_rollback)


@app.cell
def demo_schema_evolution(conn, after_rollback, mo):
    # Add a column
    conn.execute("ALTER TABLE products ADD COLUMN category VARCHAR")
    conn.execute("UPDATE products SET category = 'Dairy' WHERE name = 'Gouda'")
    conn.execute("UPDATE products SET category = 'Bakery' WHERE name = 'Hagelslag'")

    current = conn.execute("FROM products").df()
    old_snapshot = conn.execute("FROM products AT (VERSION => 2)").df()
    new_snapshots = conn.execute("FROM ducklake_snapshots('lake')").df()

    mo.vstack([
        mo.md(r"""
        ## Advanced: Schema Evolution + Time Travel

        Add a column to a live table without rewriting data.
        Old snapshots show `NULL` for the new column — the schema change is tracked
        as its own snapshot, so time travel still works correctly.

        > **In Delta Lake:** `ALTER TABLE ADD COLUMN` — identical behavior.
        > Schema history is stored in the transaction log.
        """),
        mo.md("**Current table (with new `category` column):**"), current,
        mo.md("**Snapshot 2 (before schema change — `category` is NULL):**"), old_snapshot,
        mo.md("**Full snapshot log:**"), new_snapshots,
    ])
    return (conn, current)


@app.cell
def wrapup(current, mo):
    return mo.md(r"""
    ## Summary

    | Concept | What we saw | Delta Lake equivalent |
    |---------|-------------|----------------------|
    | Immutable files | INSERT → new Parquet | `add` action in `_delta_log` |
    | Delete tracking | `-delete.parquet` appears | Deletion vectors / `remove` actions |
    | Snapshot log | `ducklake_snapshots()` | JSON files in `_delta_log/` |
    | Time travel | `AT (VERSION => N)` | `VERSION AS OF N` |
    | Change feed | `ducklake_table_changes()` | Delta Change Data Feed |
    | ACID rollback | `BEGIN / ROLLBACK` | Same — within an engine session |
    | Schema evolution | `ALTER TABLE ADD COLUMN` | Same — tracked in transaction log |

    ### When to use what?

    **Delta Lake** — distributed workloads, Spark/Databricks ecosystem,
    cloud-scale data, existing Iceberg/Delta infrastructure.

    **DuckLake** — local development, SQL-native teams, smaller scale,
    or anywhere you want a simpler operational story with no file-based metadata headaches.

    *Both solve the same fundamental problem. The lakehouse pattern is here to stay.*
    """)


if __name__ == "__main__":
    app.run()
