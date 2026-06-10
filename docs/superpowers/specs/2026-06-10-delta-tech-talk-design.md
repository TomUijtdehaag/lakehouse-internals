# Delta Lake Tech Talk — Design Spec

**Date:** 2026-06-10
**Topic:** Delta Lake: A Lakehouse Format
**Format:** Marimo notebook (`tech_talk.py`)
**Audience:** Data/software engineers

## Summary

A single Marimo notebook that teaches Delta Lake concepts through interleaved theory
(markdown + diagrams from ducklake.select) and hands-on DuckLake demo cells that
participants run step by step.

## Narrative Arc

The talk follows the DuckLake blog post's structure:
1. Background — storage/compute separation, the ACID gap in plain data lakes
2. Delta Lake — file-based transaction log (`_delta_log/`), how ACID is achieved
3. The catalog problem — why both Iceberg and Delta ended up needing a DB anyway
4. DuckLake — going all-in on SQL for metadata; used as local demo vehicle
5. Side-by-side comparison table

Then interleaved demo cells, each pairing a theory callout with runnable DuckLake code.
Every demo cell includes a "In Delta Lake, this is..." callout to map back to production.

## Notebook Structure (16 cells)

| # | Type | Content |
|---|------|---------|
| 1 | imports | import marimo, duckdb, os, shutil |
| 2 | MD | Title slide |
| 3 | MD | Background: storage/compute separation + ACID gap |
| 4 | MD | Delta Lake + Iceberg: file-based solution (with diagram) |
| 5 | MD | The catalog problem (with diagram) |
| 6 | MD | DuckLake: SQL-first metadata (with architecture + schema diagrams) |
| 7 | MD | Comparison table: Delta Lake vs DuckLake |
| 8 | Code | Demo setup: attach DuckLake, create products table |
| 9 | Code | INSERT + glob → see Parquet file appear |
| 10 | Code | DELETE + glob → see -delete.parquet appear |
| 11 | Code | ducklake_snapshots() → snapshot log |
| 12 | Code | AT (VERSION => N) → time travel |
| 13 | Code | ducklake_table_changes() → change feed |
| 14 | Code | BEGIN / DELETE / ROLLBACK → ACID demo |
| 15 | Code | ALTER TABLE ADD COLUMN + time travel → schema evolution |
| 16 | MD | Summary + when to use Delta vs DuckLake |

## Delta Lake Callouts

Each demo cell includes a comparison to the Delta Lake equivalent:
- Immutable Parquet files → `add` action in `_delta_log`
- `-delete.parquet` → deletion vectors / `remove` actions
- `ducklake_snapshots()` → JSON files in `_delta_log/`
- `AT (VERSION => N)` → `VERSION AS OF N`
- `ducklake_table_changes()` → Delta Change Data Feed
- ROLLBACK → same ACID semantics, catalog DB handles locking
- Schema evolution → `ALTER TABLE` tracked in transaction log

## Comparison Table

| | Delta Lake | DuckLake |
|---|---|---|
| Metadata store | JSON files in `_delta_log/` | SQL database (DuckDB / Postgres / SQLite) |
| ACID mechanism | Optimistic concurrency on file writes | Database transactions (native MVCC) |
| Small writes | Creates many small files, needs compaction | Optionally inlines data into catalog DB |
| Metadata queries | List + read many files (slow at scale) | Single SQL query (fast) |
| Scale target | Distributed, cloud-native, PB-scale | Local → distributed via catalog DB |
| Ecosystem | Spark, Databricks, Flink, Trino | DuckDB-centric (multi-engine on roadmap) |
| License | Apache 2.0 (Linux Foundation) | MIT (DuckDB Foundation) |
| Production since | 2019 | v1.0 April 2026 |

## Tech Stack

- `marimo>=0.10.0` (latest: 0.23.9)
- `duckdb>=1.3.0` (latest: 1.5.3, ships ducklake extension)
- Python 3.12
- `uv` for dependency management

## Demo Data

Table: `products (id INTEGER, name VARCHAR, price DECIMAL(10,2))`
Sample data: Gouda (3.49), Stroopwafel (2.19), Hagelslag (1.89)
