import requests
import json
import subprocess
import re
import base64
import os
import argparse
from tenacity import retry, wait_fixed, stop_after_attempt
from datetime import datetime, timezone

# -------------------------
# Retry-decorated GET
# -------------------------
@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def robust_get(url, headers, params=None):
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response

# -------------------------
# Cluster + Library Info
# -------------------------
def list_databricks_clusters(workspace_url, token):
    url = f"https://{workspace_url}/api/2.0/clusters/list"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = robust_get(url, headers)
        return response.json().get("clusters", [])
    except Exception as e:
        print(f"\u274c Error fetching clusters: {e}")
        return []

def list_all_cluster_libraries(workspace_url, token):
    url = f"https://{workspace_url}/api/2.0/libraries/all-cluster-statuses"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = robust_get(url, headers)
        return response.json().get("statuses", [])
    except Exception as e:
        print(f"\u274c Error fetching libraries: {e}")
        return []

def extract_cluster_info(workspace_url, token):
    clusters = list_databricks_clusters(workspace_url, token)
    library_statuses = list_all_cluster_libraries(workspace_url, token)
    lib_map = {}

    for status in library_statuses:
        cluster_id = status.get("cluster_id")
        libs = []
        for lib_status in status.get("library_statuses", []):
            lib = lib_status.get("library", {})
            info = {"status": lib_status.get("status", "UNKNOWN")}
            if 'jar' in lib: info.update(type='jar', path=lib['jar'])
            elif 'egg' in lib: info.update(type='egg', path=lib['egg'])
            elif 'whl' in lib: info.update(type='whl', path=lib['whl'])
            elif 'maven' in lib: info.update(type='maven', coordinates=lib['maven']['coordinates'])
            elif 'pypi' in lib: info.update(type='pypi', package=lib['pypi']['package'])
            libs.append(info)
        lib_map[cluster_id] = libs

    details = {}
    for cluster in clusters:
        cid = cluster['cluster_id']
        details[cluster['cluster_name']] = {
            "cluster_id": cid,
            "compute_details": {
                "state": cluster.get("state"),
                "spark_version": cluster.get("spark_version"),
                "node_type_id": cluster.get("node_type_id"),
                "driver_node_type_id": cluster.get("driver_node_type_id"),
                "cluster_memory_mb": cluster.get("cluster_memory_mb"),
                "cluster_cores": cluster.get("cluster_cores"),
                "runtime_engine": cluster.get("runtime_engine"),
                "effective_spark_version": cluster.get("effective_spark_version"),
                "release_version": cluster.get("release_version"),
            },
            "custom_libraries": lib_map.get(cid, [])
        }
    return details

# -------------------------
# SQL Warehouses
# -------------------------
def list_sql_warehouses(instance, token):
    url = f"{instance}/api/2.0/sql/warehouses"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = robust_get(url, headers)
        warehouses = response.json().get("warehouses", [])
        return [{
            "name": wh.get("name", "N/A"),
            "state": wh.get("state", "UNKNOWN"),
            "cluster_size": wh.get("cluster_size", "N/A")
        } for wh in warehouses]
    except Exception as e:
        print(f"\u274c Failed to fetch warehouses: {e}")
        return []

# -------------------------
# Unity Catalog
# -------------------------
def collect_unity_catalog_structure(workspace_url, token):
    headers = {"Authorization": f"Bearer {token}"}
    catalog_data = []
    try:
        catalogs = robust_get(f"https://{workspace_url}/api/2.1/unity-catalog/catalogs", headers).json().get("catalogs", [])
        for catalog in catalogs:
            catalog_name = catalog["name"]
            schemas = robust_get(f"https://{workspace_url}/api/2.1/unity-catalog/schemas?catalog_name={catalog_name}", headers).json().get("schemas", [])
            for schema in schemas:
                schema_name = schema["name"]
                if schema_name.lower() == "information_schema":
                    continue
                tables = robust_get(f"https://{workspace_url}/api/2.1/unity-catalog/tables?catalog_name={catalog_name}&schema_name={schema_name}", headers).json().get("tables", [])
                for table in tables:
                    catalog_data.append({
                        "catalog": catalog_name,
                        "schema": schema_name,
                        "table": table["name"],
                        "table_type": table.get("table_type")
                    })
    except Exception as e:
        print(f"\u274c Failed to fetch Unity Catalog data: {e}")
    return catalog_data

# -------------------------
# Job Runs
# -------------------------
def list_jobs_and_runs(workspace_url, token):
    headers = {"Authorization": f"Bearer {token}"}
    base_url = f"https://{workspace_url}/api/2.1/jobs"
    jobs_data = []

    try:
        jobs = robust_get(f"{base_url}/list", headers).json().get("jobs", [])
        for job in jobs:
            job_id = job.get("job_id")
            job_runs_url = f"{base_url}/runs/list?job_id={job_id}&limit=3"
            runs = robust_get(job_runs_url, headers).json().get("runs", [])
            run_details = [{
                "run_id": run.get("run_id"),
                "state": run.get("state", {}).get("life_cycle_state"),
                "result_state": run.get("state", {}).get("result_state"),
                "start_time": datetime.fromtimestamp(run.get("start_time", 0)/1000, tz=timezone.utc).isoformat(),
                "end_time": datetime.fromtimestamp(run.get("end_time", 0)/1000, tz=timezone.utc).isoformat() if run.get("end_time") else None
            } for run in runs]
            jobs_data.append({
                "job_id": job_id,
                "name": job.get("settings", {}).get("name"),
                "runs": run_details
            })
    except Exception as e:
        print(f"\u274c Failed to fetch job data: {e}")

    return jobs_data

# -------------------------
# Notebooks
# -------------------------
def detect_embedded_magics(base64_content):
    try:
        decoded = base64.b64decode(base64_content).decode("utf-8", errors="ignore")
        lines = decoded.splitlines()
        magic_pattern = re.compile(r"(?<!['\"])?%(\w+)\b")
        lang_magics = {"python", "sql", "scala", "r"}
        other_magics = {"fs", "sh", "md", "run", "pip"}
        langs, others = set(), set()
        for line in lines:
            for match in magic_pattern.findall(line.lower()):
                if match in lang_magics:
                    langs.add(match)
                elif match in other_magics:
                    others.add(match)
        return list(langs), list(others)
    except Exception:
        return [], []

def list_notebooks_for_workspace(workspace_url, token, path="/"):
    headers = {"Authorization": f"Bearer {token}"}
    list_api = f"https://{workspace_url}/api/2.0/workspace/list"
    export_api = f"https://{workspace_url}/api/2.0/workspace/export"
    status_api = f"https://{workspace_url}/api/2.0/workspace/get-status"
    notebooks = []

    def traverse(current_path):
        try:
            data = robust_get(list_api, headers, {"path": current_path}).json()
        except:
            return

        for obj in data.get("objects", []):
            obj_path = obj["path"]
            if obj["object_type"] == "NOTEBOOK":
                lang = "unknown"
                try:
                    lang = robust_get(status_api, headers, {"path": obj_path}).json().get("language", "unknown")
                except:
                    pass
                try:
                    content = robust_get(export_api, headers, {"path": obj_path, "format": "SOURCE"}).json().get("content", "")
                    embedded_langs, magics = detect_embedded_magics(content)
                except:
                    embedded_langs, magics = [], []
                notebooks.append({
                    "path": obj_path,
                    "default_language": lang,
                    "embedded_languages": embedded_langs,
                    "other_magics": magics
                })
            elif obj["object_type"] == "DIRECTORY":
                traverse(obj_path)

    traverse(path)
    return notebooks

# -------------------------
# Workspace from CLI
# -------------------------
def get_workspace_url_from_cli():
    try:
        result = subprocess.run(["az", "databricks", "workspace", "list", "--output", "json"],
                                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        workspaces = json.loads(result.stdout)
        if not workspaces:
            raise Exception("No Databricks workspace found.")
        return workspaces[0]["workspaceUrl"]
    except Exception as e:
        print("\u274c Failed to get workspace URL:", e)
        exit(1)

# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Databricks Full Environment Scanner")
    parser.add_argument("--token", required=True, help="Databricks Personal Access Token")
    args = parser.parse_args()

    token = args.token
    workspace_url = get_workspace_url_from_cli()
    instance_url = f"https://{workspace_url}"

    print(f"\U0001f9e0 Workspace: {workspace_url}")
    print("\U0001f50d Scanning clusters and libraries...")
    cluster_info = extract_cluster_info(workspace_url, token)

    print("\U0001f50d Scanning SQL warehouses...")
    sql_warehouses = list_sql_warehouses(instance_url, token)

    print("\U0001f50d Scanning Unity Catalog...")
    unity_catalog_info = collect_unity_catalog_structure(workspace_url, token)

    print("\U0001f50d Scanning job runs...")
    job_data = list_jobs_and_runs(workspace_url, token)

    print("\U0001f50d Scanning notebooks...")
    notebook_info = list_notebooks_for_workspace(workspace_url, token)

    result = {
        "workspace": workspace_url,
        "clusters": cluster_info,
        "sql_warehouses": sql_warehouses,
        "unity_catalog": unity_catalog_info,
        "jobs": job_data,
        "notebooks": notebook_info
    }

    output_file = "databricks_env_scan.json"
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)
    
    print(f"\n✅ Environment scan complete. Results saved to: {output_file}")

if __name__ == "__main__":
    main()
