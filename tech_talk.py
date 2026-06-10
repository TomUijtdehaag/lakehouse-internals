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


if __name__ == "__main__":
    app.run()
