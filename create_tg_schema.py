import pyTigerGraph as tg
import json
from pathlib import Path


def load_config(path: str = "configs/tg_config.json") -> dict:
    with open(path) as f:
        return json.load(f)


def ensure_graph_and_schema(conn: tg.TigerGraphConnection, graph_name: str):
    """Create an idempotent minimal Person graph matching the parquet schema.

    - Vertex: Person(id INT primary key, name STRING, age INT)
    - Graph: MyGraph(Person)
    """

    print(f"Ensuring schema for graph '{graph_name}' exists...")

    # Switch to GLOBAL to manage graphs
    conn.gsql("USE GLOBAL")

    # Drop and recreate the graph if it already exists (simplest idempotent approach)
    # If you prefer non-destructive, you can instead check and alter.
    drop_stmt = f"DROP GRAPH {graph_name}"
    try:
        print(f"Attempting to drop existing graph '{graph_name}' (safe to ignore errors)...")
        conn.gsql(drop_stmt)
    except Exception as e:
        print(f"Drop graph skipped/failed (likely does not exist): {e}")

    create_schema = f"""
CREATE VERTEX Person (
  PRIMARY_ID id INT,
  name STRING,
  age INT
) WITH primary_id_as_attribute="true";

CREATE GRAPH {graph_name}(Person);
"""

    print("Creating vertex and graph schema...")
    print(create_schema)
    conn.gsql(create_schema)

    print("Installing schema...")
    conn.gsql("INSTALL SCHEMA")


def ensure_loading_job(conn: tg.TigerGraphConnection, graph_name: str, job_name: str):
    """Create an idempotent loading job for Person(id,name,age)."""

    print(f"Ensuring loading job '{job_name}' for graph '{graph_name}' exists...")

    conn.gsql(f"USE GRAPH {graph_name}")

    drop_job = f"DROP JOB {job_name}"
    try:
        print(f"Attempting to drop existing loading job '{job_name}' (safe to ignore errors)...")
        conn.gsql(drop_job)
    except Exception as e:
        print(f"Drop loading job skipped/failed (likely does not exist): {e}")

    create_job = f"""
CREATE LOADING JOB {job_name} FOR GRAPH {graph_name} {{
  DEFINE FILENAME f;

  LOAD f TO VERTEX Person VALUES (
    $0,  # id
    $1,  # name
    $2   # age
  ) USING
    SEPARATOR=",",
    HEADER="true",
    EOL="\n";
}}
"""

    print("Creating loading job...")
    print(create_job)
    conn.gsql(create_job)

    print("Installing loading job...")
    conn.gsql("INSTALL LOADING JOB")


def main():
    config = load_config()

    raw_host = config.get("tg_ip", "tigergraph")
    # Ensure host has http/https scheme for pyTigerGraph
    if not raw_host.startswith(("http://", "https://")):
        host = f"http://{raw_host}"
    else:
        host = raw_host

    graph_name = config.get("tg_graph", "MyGraph")
    job_name = config.get("tg_load_job", "my_loading_job")
    username = config.get("tg_user", "tigergraph")
    password = config.get("tg_pass", "tigergraph")

    print(f"Connecting to TigerGraph at {host}:14240, graph '{graph_name}' as user '{username}'...")

    conn = tg.TigerGraphConnection(
        host=host,
        graphname=graph_name,
        username=username,
        password=password,
        restppPort=9000,
        gsPort=14240,
        useCert=False,
    )

    ensure_graph_and_schema(conn, graph_name)
    ensure_loading_job(conn, graph_name, job_name)

    print("Schema and loading job are now ensured (idempotent).")


if __name__ == "__main__":
    main()
