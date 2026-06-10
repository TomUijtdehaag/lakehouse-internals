import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell
def imports():
    import marimo as mo
    import duckdb
    import os
    import shutil

    return mo, os, shutil


@app.cell
def title():
    return


@app.cell
def background():
    return


@app.cell
def delta_iceberg():
    return


@app.cell
def catalog_problem():
    return


@app.cell
def ducklake_intro():
    return


@app.cell
def comparison():
    return


@app.cell
def demo_setup(mo, os, shutil):
    _catalog = "demo.ducklake"
    _files_dir = "demo.ducklake.files"

    # Detach any existing lake so we can delete the catalog file
    try:
        mo.sql("USE memory")
        mo.sql("DETACH lake")
    except Exception:
        pass

    if os.path.exists(_catalog):
        os.remove(_catalog)
    if os.path.exists(_files_dir):
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

    setup_done = True
    mo.md(r"""
    ## Demo Setup ✓

    A fresh local DuckLake is attached as `lake`.
    Metadata lives in `demo.ducklake` (a DuckDB file).
    Parquet data files will appear in `demo.ducklake.files/`.

    Run cells below **in order** to walk through each concept.
    """)
    return (setup_done,)


@app.cell
def demo_insert(mo, products, setup_done):

    _ = setup_done  # run after setup

    mo.sql("""
    INSERT INTO products VALUES
        (1, 'Gouda',       3.49),
        (2, 'Stroopwafel', 2.19),
        (3, 'Hagelslag',   1.89)
    """)

    data = mo.sql("FROM products")
    _files = mo.sql("FROM glob('demo.ducklake.files/**/*.parquet')")

    mo.vstack([
        mo.md(r"""
        ## Concept: Immutable Parquet Files

        After an INSERT, DuckLake writes a new **immutable Parquet file** to storage
        and records it in the SQL catalog with a new snapshot.

        > **In Delta Lake:** same thing — a Parquet file lands in your S3 prefix,
        > and `_delta_log/00...01.json` records the `add` action.
        """),
        mo.md("**Table contents:**"), data,
        mo.md("**Parquet files on disk:**"), _files,
    ])

    return (data,)


@app.cell
def demo_delete(data, mo, products):

    mo.sql("DELETE FROM products WHERE id = 2")

    after_delete = mo.sql("FROM products")
    _files = mo.sql("FROM glob('demo.ducklake.files/**/*.parquet')")

    mo.vstack([
        mo.md(r"""
        ## Concept: Deletion Files (No Overwrite)

        Deletes **never modify** the original Parquet file.
        Instead, a new `-delete.parquet` file is written that marks which rows are gone.
        The original data file is untouched — this is what enables time travel.

        > **In Delta Lake:** same pattern — a `remove` action in the log plus
        > a deletion vector (or a separate delete file in older versions).
        """),
        mo.md(f"Rows before: **{len(data)}** → after delete: **{len(after_delete)}**"),
        mo.md("**Table after delete:**"), after_delete,
        mo.md("**Files on disk (note the `-delete` file):**"), _files,
    ])

    return (after_delete,)


@app.cell
def demo_snapshots(after_delete, mo):

    _ = after_delete  # run after delete

    snapshots = mo.sql("FROM ducklake_snapshots('lake')")

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

    return (snapshots,)


@app.cell
def demo_time_travel(mo, products, snapshots):
    _ = snapshots  # run after snapshots

    # Find insert snapshot dynamically (first snapshot with only inserts)
    insert_snap_id = int(
        snapshots[
            snapshots["changes"].apply(
                lambda c: list(c.keys()) == ['tables_inserted_into'] if isinstance(c, dict) else False
            )
        ]["snapshot_id"].iloc[0]
    )

    past_state = mo.sql(f"FROM products AT (VERSION => {insert_snap_id})")
    _current = mo.sql("FROM products")

    mo.vstack([
        mo.md(r"""
        ## Concept: Time Travel

        Query any table **as it was at a previous snapshot**.
        This works because the original Parquet files are never deleted — only logically hidden.

        > **In Delta Lake:** `SELECT * FROM table VERSION AS OF 2`
        > In DuckLake: `FROM table AT (VERSION => N)` — identical concept.
        """),
        mo.md(f"Version {insert_snap_id}: **{len(past_state)} rows** (before delete) — Current: **{len(_current)} rows**"),
        mo.md(f"**Past state (version {insert_snap_id} — after insert, before delete):**"), past_state,
        mo.md("**Current state:**"), _current,
    ])
    return insert_snap_id, past_state


@app.cell
def demo_changes(mo, past_state, snapshots):
    _ = past_state  # run after time travel

    # Find insert and delete snapshot IDs dynamically
    _insert_snap_id = int(
        snapshots[
            snapshots["changes"].apply(
                lambda c: list(c.keys()) == ['tables_inserted_into'] if isinstance(c, dict) else False
            )
        ]["snapshot_id"].iloc[0]
    )
    _delete_snap_id = int(
        snapshots[
            snapshots["changes"].apply(
                lambda c: list(c.keys()) == ['tables_deleted_from'] if isinstance(c, dict) else False
            )
        ]["snapshot_id"].iloc[0]
    )

    changes = mo.sql(f"FROM ducklake_table_changes('lake', 'main', 'products', {_insert_snap_id}, {_delete_snap_id})")

    mo.vstack([
        mo.md(r"""
        ## Concept: Change Data Feed

        Retrieve exactly what changed between two snapshots — inserts, updates, deletes.
        Useful for incremental processing pipelines.

        > **In Delta Lake:** Change Data Feed (CDF) — enable with
        > `ALTER TABLE SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`,
        > then query with `table_changes('table', startVersion, endVersion)`.
        """),
        mo.md(f"Changes from snapshot {_insert_snap_id} → {_delete_snap_id}:"),
        changes,
    ])
    return (changes,)


@app.cell
def demo_rollback(changes, mo, products):

    _ = changes  # run after changes

    mo.sql("BEGIN TRANSACTION")
    mo.sql("DELETE FROM products")
    _mid_tx = mo.sql("FROM products")
    mo.sql("ROLLBACK")
    after_rollback = mo.sql("FROM products")

    mo.vstack([
        mo.md(r"""
        ## Concept: ACID Transactions

        Wrap multiple operations in a transaction. If something goes wrong, `ROLLBACK`
        undoes everything atomically — no partial writes in the lake.

        > **In Delta Lake:** multi-statement transactions are supported within a single
        > engine session. Cross-engine transactions require a catalog with locking support.
        > In DuckLake, the catalog DB handles locking natively.
        """),
        mo.md("**During transaction (after DELETE, before COMMIT):**"), _mid_tx,
        mo.md("**After ROLLBACK — data is back:**"), after_rollback,
    ])

    return (after_rollback,)


@app.cell
def demo_schema_evolution(after_rollback, insert_snap_id, mo, products):
    _ = after_rollback  # run after rollback

    # Drop column if already present (idempotent re-run support)
    mo.sql("ALTER TABLE products DROP COLUMN IF EXISTS category")
    mo.sql("ALTER TABLE products ADD COLUMN category VARCHAR")
    mo.sql("UPDATE products SET category = 'Dairy' WHERE name = 'Gouda'")
    mo.sql("UPDATE products SET category = 'Bakery' WHERE name = 'Hagelslag'")

    current = mo.sql("FROM products")
    _old_snapshot = mo.sql(f"FROM products AT (VERSION => {insert_snap_id})")
    _new_snapshots = mo.sql("FROM ducklake_snapshots('lake')")

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
        mo.md(f"**Snapshot {insert_snap_id} (before schema change — `category` is NULL):** {_old_snapshot.shape}"), _old_snapshot,
        mo.md("**Full snapshot log:**"), _new_snapshots,
    ])
    return (current,)


@app.cell
def wrapup(current, mo):

    _ = current  # run after schema evolution

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
