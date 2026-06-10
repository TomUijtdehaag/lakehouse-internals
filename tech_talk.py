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


if __name__ == "__main__":
    app.run()
