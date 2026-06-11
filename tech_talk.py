import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def imports():
    import marimo as mo
    import duckdb
    import shutil
    import json
    from pathlib import Path


    def tree(path, prefix=""):
        """Return a pretty file tree as a list of strings."""
        lines = []
        entries = sorted(path.iterdir(), key=lambda p: p.name)
        for i, entry in enumerate(entries):
            is_last = i == len(entries) - 1
            connector = "\u2514\u2500\u2500 " if is_last else "\u251c\u2500\u2500 "
            lines.append(prefix + connector + entry.name + ("/" if entry.is_dir() else ""))
            if entry.is_dir():
                extension = "    " if is_last else "\u2502   "
                lines += tree(entry, prefix + extension)
        return lines

    return Path, duckdb, json, mo, shutil, tree


@app.cell(hide_code=True)
def title(mo):
    mo.md(r"""
    # Delta Lake: A Lakehouse Format

    ### A Hands-on Tech Talk with DuckLake

    This notebook walks through the key concepts behind Delta Lake —
    an open table format that brings ACID transactions to data lakes.

    We demo the concepts using **DuckLake**, a modern lakehouse format
    that implements the same ideas with a SQL database instead of JSON files.
    Every demo cell includes an **"In Delta Lake, this is..."** callout.
    """)
    return


@app.cell(hide_code=True)
def background(mo):
    mo.md(r"""
    ## Background: The Problem

    ### Storage / Compute Separation

    Modern data platforms separate **storage** (S3, ADLS, GCS) from **compute**
    (Spark, Trino, DuckDB). Cheap storage + independent scaling — but plain object
    storage gives you no transactions.

    ### The ACID Gap

    Plain data lakes store raw files in S3. Without a format layer:

    - **No isolation** — two writers can corrupt each other
    - **No rollback** — a failed job leaves partial data
    - **No history** — you cannot query yesterday's state
    - **No schema enforcement** — anyone can add or drop columns silently

    **Lakehouse formats** (Delta Lake, Iceberg, Hudi, DuckLake) fill this gap.
    """)
    return


@app.cell(hide_code=True)
def delta_lake(mo):
    mo.md(r"""
    ## Delta Lake: File-Based Solution

    Delta Lake stores all metadata as **JSON files in `_delta_log/`**.
    Each file represents one commit — a set of `add` / `remove` actions on Parquet files.

    ### How ACID Works

    | ACID property | Delta Lake mechanism |
    |---|---|
    | **Atomicity** | Commit = single new JSON file; either written or not |
    | **Consistency** | Schema checked before each commit |
    | **Isolation** | Optimistic concurrency on JSON file names |
    | **Durability** | S3 is durable; commit files are never overwritten |

    ### Time Travel

    Every commit is preserved. `VERSION AS OF N` replays the log to that point.
    Old Parquet files are never deleted until explicitly vacuumed with `VACUUM`.
    """)
    return


@app.cell(hide_code=True)
def delta_write_view(Path, mo, shutil, tree):
    import json as _json
    import pyarrow as _pa
    from deltalake import write_deltalake as _write_deltalake, DeltaTable

    mo.sql("LOAD delta")

    delta_path = Path("demo.delta")
    if delta_path.exists():
        shutil.rmtree(delta_path)

    _data = _pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Gouda", "Stroopwafel", "Hagelslag"],
            "price": [3.49, 2.19, 1.89],
        }
    )
    _write_deltalake(delta_path, _data)
    DeltaTable(delta_path).delete("id = 2")

    _tree_str = delta_path.name + "/\n" + "\n".join(tree(delta_path))

    mo.vstack(
        [
            mo.md("""
            1. **Commit 0** -- `write_deltalake(path, data)` writes 3 rows as one Parquet file, appends `_delta_log/0...0.json` with `add` action.
            2. **Commit 1** -- `DeltaTable(path).delete("id = 2")` rewrites the Parquet without row 2, appends `_delta_log/0...1.json` with `add` + `remove` actions.
    """),
            mo.md("**Current state via `delta_scan` -- 2 rows:**"),
            mo.sql(f"FROM delta_scan('{delta_path}')"),
            mo.md("**Files on disk -- 2 Parquet files (original + rewrite), 2 log entries:**"),
            mo.md(f"```\n{_tree_str}\n```"),
        ]
    )
    return (delta_path,)


@app.cell(hide_code=True)
def _(delta_path, mo):
    preview_data = mo.ui.file_browser(delta_path, multiple=False)
    return (preview_data,)


@app.cell(hide_code=True)
def _(mo, preview_data):
    _path = preview_data.path()

    mo.vstack([preview_data] + ([mo.sql(f"from '{_path}'")] if (_path and _path.suffix == ".parquet") else []))
    return


@app.cell(hide_code=True)
def delta_log_view(delta_path, json, mo):
    def _read_log(path):
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


    _log_dir = delta_path / "_delta_log"
    _log0 = _read_log(_log_dir / "00000000000000000000.json")
    _log1 = _read_log(_log_dir / "00000000000000000001.json")

    mo.vstack(
        [
            mo.md("### The Transaction Log"),
            mo.md("### `00000000000000000000.json` — initial write"),
            mo.md(
                "- `commitInfo` — who wrote it, operation, metrics\n"
                "- `protocol` — minimum reader/writer version\n"
                "- `metaData` — table ID, schema (JSON-in-JSON), partition columns\n"
                "- `add` — Parquet file path, size, row stats"
            ),
            _log0,
            mo.md("### `00000000000000000001.json` — delete"),
            mo.md(
                "- `commitInfo` — operation = DELETE, predicate = `id = 2`\n"
                "- `add` — **new** Parquet file (rows 1 and 3 rewritten)\n"
                "- `remove` — **old** Parquet file logically retired (still on disk)\n\n"
                "Both Parquet files remain on disk. The log determines current state."
            ),
            _log1,
            mo.md(
                "> **Key takeaway:** every operation appends a new JSON file. "
                "To know current state you replay the entire log. "
                "At scale this becomes slow — which is why catalogs and checkpoints exist. "
                "**DuckLake replaces this whole log with SQL rows.**"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def iceberg_format(mo):
    mo.md(r"""
    ## Apache Iceberg: A More Complex Hierarchy

    Iceberg adds four metadata layers between catalog and data:

    ![Iceberg table architecture](https://ducklake.select/images/manifesto/iceberg-table-architecture.png)

    1. **Catalog** — maps table name → current `metadata.json` path
    2. **Metadata file** — table schema, partition spec, and snapshot list
    3. **Manifest list** — one per snapshot; lists manifest files for that snapshot
    4. **Manifest files** — lists Parquet data files with column stats and partition values

    ### Key Concepts

    A **partition spec** defines how rows are grouped into files.
    `PARTITIONED BY (month(event_date))` puts each calendar-month's rows in the same file.
    Readers skip entire files whose partition value doesn't match the query predicate — without
    reading any data.

    A **snapshot** is an immutable, point-in-time view of the table: a specific manifest list
    capturing exactly which data files existed at that instant.
    Every write creates a new snapshot. Time travel = load an older snapshot.
    Old snapshots are retained until explicitly expired with `expire_snapshots`.

    ### Why Manifests? The Problem with Delta-Style Replay

    Delta Lake's flat log works well at small scale but hits three walls as tables grow:

    | Problem | Delta Log | Iceberg Manifests |
    |---|---|---|
    | **Current state** | Replay all commits since last checkpoint | One pointer hop: snapshot → manifest list → files |
    | **File listing** | Must list `_delta_log/` + data prefix (slow on S3) | File set is pre-recorded in manifest files — no listing |
    | **Partition pruning** | Scan every `add` entry to find matching files | Skip whole manifest files whose partition range doesn't overlap |
    | **Column stats** | Embedded in log JSON alongside unrelated commit info | Stored per data file in manifest — easy to vectorise |
    | **Manifest reuse** | N/A | Unchanged partitions reuse existing manifest files across snapshots |

    At billions of files and thousands of commits, the difference between
    "read 10 manifest files" and "replay 50,000 log entries" dominates query planning time.

    ### The Catalog Layer

    ![Iceberg catalog architecture](https://ducklake.select/images/manifesto/iceberg-catalog-architecture.png)

    The catalog atomically swaps the `metadata.json` pointer on each commit, providing isolation.
    Without a catalog there is no authoritative "current version" of the table.

    > **vs Delta Lake:** Delta Lake has a single flat `_delta_log/` directory —
    > no separate catalog needed for basic single-engine table operations.
    > Iceberg's hierarchy enables partition pruning at metadata read time,
    > but always requires an external catalog service.
    """)
    return


@app.cell(hide_code=True)
def catalog_problem(mo):
    mo.md(r"""
    ## The Catalog Problem

    Both Delta Lake and Iceberg evolved to **need a catalog** — a service that
    tracks which tables exist and where their metadata lives.

    Without a catalog:

    - Listing tables requires scanning filesystem prefixes
    - Table renames / drops need out-of-band coordination
    - Discovery across writers is slow and race-prone

    Popular catalogs: **Hive Metastore**, **AWS Glue**, **Unity Catalog**, **Polaris**

    > **The irony:** we moved from databases (data warehouses) to files on S3 for
    > scale — and then had to add a database back to keep track of all those files.
    """)
    return


@app.cell(hide_code=True)
def ducklake_intro(mo):
    mo.md(r"""
    ## DuckLake: Go All-In on SQL for Metadata

    DuckLake asks: *what if metadata lived in a real SQL database from the start?*

    - Data still lives in **open Parquet files** (S3 / local disk)
    - Metadata lives in a **SQL catalog** (DuckDB, PostgreSQL, or SQLite)
    - No JSON log files, no separate catalog service, no file listing

    ![DuckLake architecture](https://ducklake.select/images/manifesto/ducklake-architecture.png)

    ### The Catalog Schema

    Snapshots, files, columns, and partition info are SQL tables.
    Metadata queries are instant SQL — no file listing needed.

    ![DuckLake schema](https://ducklake.select/images/manifesto/ducklake-schema-1.png)

    > **For this demo:** one `.ducklake` file = the SQL catalog,
    > `demo.ducklake.files/` = the Parquet data directory.
    > In production: swap in Postgres + S3.
    """)
    return


@app.cell(hide_code=True)
def comparison(mo):
    mo.md(r"""
    ## Delta Lake vs Iceberg vs DuckLake

    |  | Delta Lake | Iceberg | DuckLake |
    |---|---|---|---|
    | Metadata store | JSON files in `_delta_log/` | Manifest list → manifest files → data files | SQL database (DuckDB / Postgres / SQLite) |
    | Metadata layers | 1 flat directory | 4 (catalog → metadata → manifest list → manifests) | 1 (SQL rows) |
    | ACID mechanism | Optimistic concurrency on file writes | Optimistic concurrency via catalog | Database transactions (native MVCC) |
    | Catalog required | No (optional, for multi-engine) | Yes, always | Yes (catalog == metadata) |
    | Small writes | CoW: rewrites entire file | MoR: delete files + rewrite on compact | MoR: delete files + inline option |
    | Metadata queries | Scan `_delta_log/` from last checkpoint | Read manifest files (partition-prunable) | Single SQL query |
    | Partition pruning | Scan every `add` entry | Skip manifests by partition spec | `WHERE` on any column |
    | Scale target | Distributed, PB-scale | Distributed, PB-scale | S3 data + Postgres catalog → distributed |
    | Ecosystem | Spark, Databricks, Flink, Trino | Spark, Flink, Trino, Dremio, Hive | DuckDB-centric (multi-engine roadmap) |
    | License | Apache 2.0 (Linux Foundation) | Apache 2.0 (Apache Foundation) | MIT (DuckDB Foundation) |
    | Production since | 2019 | 2018 (Netflix) | v1.0 April 2026 |

    ---
    **The demo below uses DuckLake.** Every cell maps the concept back to Delta Lake and Iceberg.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Demo
    """)
    return


@app.cell(hide_code=True)
def _(Path, mo, shutil):
    # demo setup
    _catalog = "demo.ducklake"
    _files_dir = "demo.ducklake.files"

    # Detach any existing lake so we can delete the catalog file
    try:
        mo.sql("USE memory")
        mo.sql("DETACH lake")
    except Exception:
        pass

    if Path(_catalog).exists():
        Path(_catalog).unlink()
    if Path(_files_dir).exists():
        shutil.rmtree(_files_dir)

    mo.sql("INSTALL ducklake")
    mo.sql("LOAD ducklake")
    # DATA_INLINING_ROW_LIMIT 0 disables inlining so inserts go directly to Parquet files
    mo.sql(f"ATTACH 'ducklake:{_catalog}' AS lake (DATA_INLINING_ROW_LIMIT 0)")
    mo.sql("USE lake")
    mo.sql("""
        CREATE OR REPLACE TABLE products (
            id    INTEGER,
            name  VARCHAR,
            price DECIMAL(10, 2)
        )
    """)
    return


@app.cell
def insert_data(mo, products):
    data = mo.sql(
        f"""
        INSERT INTO
            products
        VALUES
            (1, 'Gouda', 3.49),
            (2, 'Stroopwafel', 2.19),
            (3, 'Hagelslag', 1.89);

        FROM products
        """
    )
    return (data,)


@app.cell(hide_code=True)
def demo_insert(Path, mo):
    _files = sorted(Path("demo.ducklake.files").rglob("*.parquet"))

    mo.vstack(
        [
            mo.md(r"""
    ## Concept: Immutable Parquet Files

    After an INSERT, DuckLake writes a new **immutable Parquet file** to storage
    and records it in the SQL catalog with a new snapshot.

    > **In Delta Lake:** same thing — a Parquet file lands in your S3 prefix,
    > and `_delta_log/00...01.json` records the `add` action.
    """),
            mo.md("**Parquet files on disk:**"),
            mo.plain_text("\n".join(str(f) for f in _files)),
        ]
    )
    return


@app.cell
def _(mo, products):
    after_delete = mo.sql(
        f"""
        DELETE FROM products WHERE id = 2;

        FROM products;
        """
    )
    return (after_delete,)


@app.cell(hide_code=True)
def demo_delete(Path, after_delete, data, duckdb, mo):
    _files = sorted(Path("demo.ducklake.files").rglob("*.parquet"))
    _delete_files = [f for f in _files if f.name.endswith("-delete.parquet")]

    # Read the first delete file; shorten the absolute path for display
    _del_df = duckdb.sql(f"FROM '{_delete_files[0]}'").df()
    _del_df["file_path"] = _del_df["file_path"].apply(lambda p: Path(p).name)

    mo.vstack(
        [
            mo.md(r"""
    ## Concept: Deletion Files (No Overwrite)

    Deletes **never modify** the original Parquet file.
    Instead, a new `-delete.parquet` file is written that records *which rows* are gone.
    The original data file is untouched — this is what enables time travel.

    > **In Delta Lake:** a `remove` action retires the whole file;
    > a rewritten file (without the deleted rows) is added. Same immutability guarantee,
    > different mechanism — file-level vs row-level.
    """),
            mo.md(f"Rows before: **{len(data)}** → after delete: **{len(after_delete)}**"),
            mo.md("**Files on disk:**"),
            mo.plain_text("\n".join(str(f) for f in _files)),
            mo.md("**What's inside the `-delete.parquet`:**"),
            _del_df,
            mo.md(
                "- `file_path` — which data file this deletion applies to\n"
                "- `pos` — 0-indexed row number to skip (`pos = 1` = second row = `id = 2`)\n\n"
                "At read time the engine loads the data file, checks the delete file, "
                "and filters out any row whose position appears here. "
                "No data is ever rewritten."
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def demo_snapshots(after_delete, mo):
    _ = after_delete  # run after delete

    snapshots = mo.sql("FROM ducklake_snapshots('lake')")

    mo.vstack(
        [
            mo.md(r"""
        ## Concept: The Snapshot Log

        Every write (INSERT, DELETE, UPDATE, schema change) creates a new **snapshot**.
        The snapshot log is the source of truth for what the table looked like at any point.

        > **In Delta Lake:** each snapshot corresponds to one JSON file in `_delta_log/`.
        > In DuckLake, it's rows in the `ducklake_snapshot` SQL table — much cheaper to query.
        """),
            snapshots,
        ]
    )
    return (snapshots,)


@app.cell(hide_code=True)
def _(mo, snapshots):
    # Find insert and delete snapshot IDs dynamically
    insert_snap_id = int(
        snapshots[
            snapshots["changes"].apply(
                lambda c: list(c.keys()) == ["tables_inserted_into"] if isinstance(c, dict) else False
            )
        ]["snapshot_id"].iloc[0]
    )
    delete_snap_id = int(
        snapshots[
            snapshots["changes"].apply(
                lambda c: list(c.keys()) == ["tables_deleted_from"] if isinstance(c, dict) else False
            )
        ]["snapshot_id"].iloc[0]
    )


    _snapshot_ids = snapshots.snapshot_id.to_list()
    snapshot_id = mo.ui.dropdown(_snapshot_ids, label="snapshot_id", value=insert_snap_id)
    return delete_snap_id, insert_snap_id, snapshot_id


@app.cell(hide_code=True)
def _(mo):
    mo.md(rf"""
    ## Concept: Time Travel

    Query any table **as it was at a previous snapshot**.
    This works because the original Parquet files are never deleted — only logically hidden.

    > **In Delta Lake:**
    > ```sql
    > SELECT * FROM table VERSION AS OF N
    > ```
    > In DuckLake:
    > ```sql
    > FROM table AT (VERSION => N)
    > ```
    > — identical concept.
    """)
    return


@app.cell(hide_code=True)
def _(insert_snap_id, mo, snapshot_id):
    mo.md(rf"""
    **Past state (version {insert_snap_id} — after insert, before delete):**

    {snapshot_id}
    """)
    return


@app.cell
def _(mo, products, snapshot_id):
    time_travel = mo.sql(
        f"""
        FROM products AT (VERSION => {snapshot_id.value})
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Current state:**
    """)
    return


@app.cell
def _(mo, products):
    _df = mo.sql(
        f"""
        FROM products -- current version
        """
    )
    return


@app.cell(hide_code=True)
def demo_changes(mo):
    mo.md(r"""
    ## Concept: Change Data Feed

    Retrieve exactly what changed between two snapshots — inserts, updates, deletes.
    Useful for incremental processing pipelines.

    > **In Delta Lake:** Change Data Feed (CDF) — enable with
    > `ALTER TABLE SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`,
    > then query with `table_changes('table', startVersion, endVersion)`.
    """)
    return


@app.cell(hide_code=True)
def _(delete_snap_id, insert_snap_id, mo):
    mo.md(rf"""
    Changes from snapshot {insert_snap_id} → {delete_snap_id}:
    """)
    return


@app.cell
def _(delete_snap_id, mo):
    changes = mo.sql(
        f"""
        FROM main.table_changes('products', {delete_snap_id} , {delete_snap_id})
        """
    )
    return (changes,)


@app.cell
def demo_rollback(changes, mo, products):
    _ = changes  # run after changes

    mo.sql("BEGIN TRANSACTION")
    mo.sql("DELETE FROM products")
    mid_tx = mo.sql("FROM products")
    mo.sql("ROLLBACK")
    after_rollback = mo.sql("FROM products")
    return after_rollback, mid_tx


@app.cell(hide_code=True)
def _(after_rollback, mid_tx, mo):
    mo.vstack(
        [
            mo.md(r"""
    ## Concept: ACID Transactions

    Wrap multiple operations in a transaction. If something goes wrong, `ROLLBACK`
    undoes everything atomically — no partial writes in the lake.

    > **In Delta Lake:** multi-statement transactions are supported within a single
    > engine session. Cross-engine transactions require a catalog with locking support.
    > In DuckLake, the catalog DB handles locking natively.
    """),
            mo.md("**During transaction (after DELETE, before COMMIT):**"),
            mid_tx,
            mo.md("**After ROLLBACK — data is back:**"),
            after_rollback,
        ]
    )
    return


@app.cell(hide_code=True)
def _(after_rollback, insert_snap_id, mo, products):
    _ = after_rollback  # run after rollback

    schema_old_snap = mo.sql(f"FROM products AT (VERSION => {insert_snap_id})")
    all_snapshots = mo.sql("FROM ducklake_snapshots('lake')")

    mo.md(r"""
    ## Advanced: Schema Evolution + Time Travel

    Add a column to a live table without rewriting data.
    The schema change is tracked as its own snapshot, so time travel still works correctly.

    > **In Delta Lake:** 
    > ```sql
    > ALTER TABLE ADD COLUMN
    > ```
    > — identical behavior.
    > Schema history is stored in the transaction log.
    """)
    return all_snapshots, schema_old_snap


@app.cell
def _(mo, products):
    schema_evolution = mo.sql(
        f"""
        ALTER TABLE products
        DROP COLUMN IF EXISTS category;

        ALTER TABLE products
        ADD COLUMN category VARCHAR;

        UPDATE products
        SET
            category = 'Dairy'
        WHERE
            name = 'Gouda';

        UPDATE products
        SET
            category = 'Bakery'
        WHERE
            name = 'Hagelslag';

        FROM
            products
        """
    )
    return (schema_evolution,)


@app.cell(hide_code=True)
def _(all_snapshots, delete_snap_id, mo, schema_evolution, schema_old_snap):
    mo.vstack(
        [
            mo.md("**Current table (with new `category` column):**"),
            schema_evolution,
            mo.md(f"**Snapshot {delete_snap_id} (before schema change):** {schema_old_snap.shape}"),
            schema_old_snap,
            mo.md("**Full snapshot log:**"),
            all_snapshots,
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Scaling
    """)
    return


@app.cell(hide_code=True)
def scale_control(mo):
    batches = mo.ui.slider(1, 10, value=5, label="Batches", show_value=True)
    scale = mo.ui.slider(3, 9, value=7, label="Scale ( x 10 ^ s )", show_value=True)
    batch_btn = mo.ui.run_button(label="Insert batches")
    partition_by = mo.ui.dropdown(["none", "year", "category"], value="none", label="Partition by")

    mo.vstack(
        [
            mo.md("## Scale & Partitioning"),
            mo.hstack([scale, batches, batch_btn]),
            partition_by,
        ]
    )
    return batch_btn, batches, partition_by, scale


@app.cell
def _(mo, scale):
    batch_size = 10**scale.value
    mo.stat(batch_size)
    return (batch_size,)


@app.cell(hide_code=True)
def _(Path, batch_btn, batch_size, batches, mo, partition_by, shutil, tree):
    _ = batch_btn.value

    if batch_btn.value:
        _batch = batch_size
        _part_col = partition_by.value

        mo.sql("DROP TABLE IF EXISTS scale_demo")

        # Remove physical files so old partition layout doesn't linger
        _data_path = Path("demo.ducklake.files/main/scale_demo")
        if _data_path.exists():
            shutil.rmtree(_data_path)

        mo.sql("""
            CREATE TABLE scale_demo (
                id INTEGER,
                amount INTEGER,
                order_date DATE,
                category VARCHAR
            )
        """)

        if _part_col != "none":
            _transforms = {"year": "year(order_date)", "category": "category"}
            mo.sql(f"ALTER TABLE scale_demo SET PARTITIONED BY ({_transforms[_part_col]})")

        for _b in range(batches.value):
            _offset = _b * _batch
            mo.sql(f"""
                INSERT INTO scale_demo (id, amount, order_date, category)
                SELECT generate_series + {_offset}, floor(random() * 5 + 1),
                       DATE '2024-01-01' + (random() * 730)::INT,
                       CASE (generate_series % 3)
                           WHEN 0 THEN 'electronics'
                           WHEN 1 THEN 'clothing'
                           ELSE 'food'
                       END
                FROM generate_series(1, {_batch})
            """)

    else:
        _data_path = Path("demo.ducklake.files/main/scale_demo")
        mo.sql("""
            CREATE TABLE IF NOT EXISTS scale_demo (
                id INTEGER, amount INTEGER, order_date DATE, category VARCHAR
            )
        """)

    _info = mo.sql("FROM ducklake_table_info('lake') WHERE table_name = 'scale_demo'")
    _total = mo.sql("SELECT COUNT(*) AS n FROM scale_demo")

    _fpv = None
    if partition_by.value != "none":
        _fpv = mo.sql("""
            SELECT p.partition_value, d.file_size_bytes, d.record_count
            FROM __ducklake_metadata_lake.ducklake_file_partition_value p
            JOIN __ducklake_metadata_lake.ducklake_data_file d ON p.data_file_id = d.data_file_id
            WHERE d.table_id = (
                SELECT table_id FROM __ducklake_metadata_lake.ducklake_table
                WHERE table_name = 'scale_demo' AND end_snapshot IS NULL
            )
            ORDER BY p.partition_value
        """)

    # Benchmark: partition pruning on category = 'electronics'
    import time as _time

    _t0 = _time.perf_counter()
    mo.sql("SELECT SUM(amount) AS total FROM scale_demo")
    _t_full_ms = (_time.perf_counter() - _t0) * 1000

    _t0 = _time.perf_counter()
    mo.sql("SELECT SUM(amount) AS total FROM scale_demo WHERE category = 'electronics'")
    _t_filtered_ms = (_time.perf_counter() - _t0) * 1000

    _total_files = int(_info["file_count"].iloc[0])
    if partition_by.value == "category" and _fpv is not None and len(_fpv) > 0:
        _matched_files = int((_fpv["partition_value"] == "electronics").sum())
    else:
        _matched_files = _total_files

    if _data_path.exists():
        _tree_str = _data_path.name + "/\n" + "\n".join(tree(_data_path))
    else:
        _tree_str = "(no files yet)"

    mo.vstack(
        [
            mo.md(f"**{_total['n'].iloc[0]:,} rows | {_total_files} file{'s' if _total_files != 1 else ''}**"),
            _info,
        ]
        + ([mo.md("**Files per partition:**"), _fpv] if _fpv is not None else [])
        + [
            mo.md("**Files on disk:**"),
            mo.md(f"```\n{_tree_str}\n```"),
            mo.md("---"),
            mo.md("**Benchmark: `SELECT SUM(amount) WHERE category = 'electronics'`**"),
            mo.hstack(
                [
                    mo.stat(f"{_t_full_ms:.0f} ms", label="Full scan (all files)"),
                    mo.stat(f"{_t_filtered_ms:.0f} ms", label="Filtered"),
                    mo.stat(f"{_matched_files} / {_total_files}", label="Files read by filter"),
                ]
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Metadata: SQL Tables, Not Files
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md(r"""
    The DuckLake catalog stores **all** table metadata as SQL rows.
    Compare: one DuckLake query vs Iceberg's four-layer manifest hierarchy.

    **`ducklake_table_info('lake')`** — file count, byte size, delete files:
    """),
            mo.sql("FROM ducklake_table_info('lake')"),
            mo.md(r"""
    **Schema via `information_schema.columns`** — standard SQL introspection:
    """),
            mo.sql("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'products' AND table_schema = 'main'
            """),
            mo.md(r"""
    > To get this without SQL metadata:  
    > 1. List all files on S3 (paginated API calls — slow at scale)  
    > 2. Read every Parquet footer for schema + row count + stats  
    > 3. Replay the Delta Lake transaction log from last checkpoint  
    >
    > **DuckLake:** one query, instant.  
    > **Iceberg:** manifest files cache this (better than raw S3, but still file I/O).
    """),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md(r"""
    **Parquet footers — what you'd scan without a catalog:**

    Per-column `min` / `max` stats are embedded in every Parquet file.
    Iceberg mirrors this in manifest files for partition pruning.
    DuckLake skips it entirely — metadata is already in SQL.
    """),
            mo.sql("""
                SELECT DISTINCT ON (file_name, path_in_schema)
                    path_in_schema AS column,
                    stats_min_value AS min,
                    stats_max_value AS max,
                    compression
                FROM parquet_metadata('demo.ducklake.files/**/*.parquet')
                WHERE path_in_schema NOT LIKE '%_ducklake%'
                ORDER BY file_name, path_in_schema
            """),
            mo.md(r"""
    > In Iceberg, manifest files replicate this per-file column-level metadata so readers know which files to **skip** without downloading them.
    > DuckLake cuts the middleman: metadata is already in SQL, queryable with `WHERE` on any column.
    """),
        ]
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "__ducklake_metadata_lake"."ducklake_schema" LIMIT 100
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "__ducklake_metadata_lake"."ducklake_table" LIMIT 100
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "__ducklake_metadata_lake"."ducklake_column" LIMIT 100
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "__ducklake_metadata_lake"."ducklake_table_column_stats" LIMIT 100
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "__ducklake_metadata_lake"."ducklake_snapshot" LIMIT 100
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "__ducklake_metadata_lake"."ducklake_table_stats" LIMIT 100
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "__ducklake_metadata_lake"."ducklake_data_file" LIMIT 100
        """
    )
    return


@app.cell(hide_code=True)
def wrapup(mo):
    mo.md(r"""
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
    return


if __name__ == "__main__":
    app.run()
