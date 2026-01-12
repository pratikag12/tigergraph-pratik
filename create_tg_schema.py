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

    # Work in GLOBAL scope to manage graphs and global vertex types
    conn.gsql("USE GLOBAL")

    # Drop graph if it exists (safe to ignore failures)
    drop_graph = f"DROP GRAPH {graph_name}"
    try:
        print(f"Attempting to drop existing graph '{graph_name}' (safe to ignore errors)...")
        conn.gsql(drop_graph)
    except Exception as e:
        print(f"Drop graph skipped/failed (likely does not exist): {e}")

    # Drop Person vertex type if it exists so we can recreate it cleanly
    drop_vertex = "DROP VERTEX Person CASCADE"
    try:
        print("Attempting to drop existing vertex type 'Person' (safe to ignore errors)...")
        conn.gsql(drop_vertex)
    except Exception as e:
        print(f"Drop vertex skipped/failed (likely does not exist): {e}")

    # Create an empty graph. We will use a SCHEMA_CHANGE JOB to define the schema.
    # This is often more reliable for ensuring the schema version is bumped and installed.
    create_graph = f"CREATE GRAPH {graph_name}()"

    print("Creating (empty) graph definition...")
    print(create_graph)
    print(conn.gsql(create_graph))

    schema_job_name = f"change_{graph_name.lower()}_person"
    # Note: defining Person inside the schema change job makes it local to the graph
    schema_change = f"""
USE GRAPH {graph_name}

CREATE SCHEMA_CHANGE JOB {schema_job_name} {{
  ADD VERTEX Person (
    PRIMARY_ID id INT,
    name STRING,
    age INT
  ) WITH primary_id_as_attribute="true";
}}

RUN SCHEMA_CHANGE JOB {schema_job_name}
DROP JOB {schema_job_name}
"""

    print("Running schema change job to create vertex and install schema...")
    print(schema_change)
    res = conn.gsql(schema_change)
    print(res)

    # Verify schema availability via RESTPP logic (schema version check)
    print("Verifying schema version...")
    import time
    for i in range(10):
        try:
            # We can inspect the graph schema using pyTigerGraph's schema methods
            # However, retrieving the raw schema JSON is a good proxy for "RESTPP knows about it"
            # We'll use a protected method or just assume a short sleep + getSchema is enough
            schema = conn.getSchema(graph_name)
            if schema:
                print("Schema detected by standard API.")
                break
        except Exception as e:
            print(f"Waiting for schema propagation ({i}/10)... ({e})")
            time.sleep(2)


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

    import time
    conn = None
    for i in range(30):
        try:
            conn = tg.TigerGraphConnection(
                host=host,
                graphname=graph_name,
                username=username,
                password=password,
                restppPort=9000,
                gsPort=14240,
                useCert=False,
            )
            # Try a lightweight call to ensure it's really up
            conn.gsql("ls")
            print("Successfully connected to TigerGraph.")
            break
        except Exception as e:
            if i % 5 == 0:
                print(f"Connection failed (TigerGraph might be starting up): {e}")
            print(f"Retrying in 2 seconds... ({i+1}/30)")
            time.sleep(2)
    
    if not conn:
        print("Failed to connect to TigerGraph after multiple attempts.")
        return

    ensure_graph_and_schema(conn, graph_name)
    ensure_loading_job(conn, graph_name, job_name)

    print("Schema and loading job are now ensured (idempotent).")


if __name__ == "__main__":
    main()
