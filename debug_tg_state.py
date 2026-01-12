import pyTigerGraph as tg
import json

def main():
    print("--- Loading Config ---")
    try:
        with open("configs/tg_config.json") as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        return

    host = config.get("tg_ip", "tigergraph")
    graph = config.get("tg_graph", "MyGraph")
    user = config.get("tg_user", "tigergraph")
    password = config.get("tg_pass", "tigergraph")
    
    conn_url = f"http://{host}" if not host.startswith("http") else host
    print(f"Connecting to {conn_url} for graph {graph}...")

    try:
        conn = tg.TigerGraphConnection(
            host=conn_url,
            graphname=graph,
            username=user,
            password=password,
            restppPort=9000,
            gsPort=14240,
            useCert=False
        )
        print("Connection object created.")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print("\n--- checking gsql 'ls' ---")
    try:
        res = conn.gsql("ls")
        print(res)
    except Exception as e:
        print(f"GSQL 'ls' failed: {e}")

    print("\n--- checking gsql 'USE GRAPH " + graph + "; ls' ---")
    try:
        res = conn.gsql(f"USE GRAPH {graph}\nls")
        print(res)
    except Exception as e:
        print(f"GSQL 'ls' for graph failed: {e}")

    print("\n--- checking RESTPP /endpoints (via connectivity check if possible) ---")
    # minimal check to see if we can hit restpp
    try:
        # pyTigerGraph doesn't expose raw restpp request easily for arbitrary paths, 
        # but we can try getSchema which hits endpoints usually.
        schema = conn.getSchema(graph)
        print("Schema retrieved via REST:")
        print(json.dumps(schema, indent=2))
    except Exception as e:
        print(f"getSchema failed: {e}")

if __name__ == "__main__":
    main()
