# databricks-discovery-tool

## Overview

This tool is a Python script designed to scan and extract detailed metadata from a Databricks workspace. It collects and summarizes information including:

- Active clusters and their libraries
- SQL warehouses
- Unity Catalog metadata (catalogs, schemas, tables)
- Jobs and recent runs
- Workspace notebooks and embedded magic commands

The script outputs a consolidated JSON summary file named `databricks_workspace_summary.json`.

---

## Features

- Uses Databricks REST APIs to fetch workspace data
- Retries API requests up to 3 times for robustness
- Supports auto-detection of workspace URL using Azure CLI (via `az databricks workspace list`)
- Parses notebook content to detect embedded languages and magic commands
- Summarizes clusters, libraries, SQL warehouses, Unity Catalog, jobs, and notebooks

---

## Prerequisites

- Python 3.7+
- Installed Python packages:
  - `requests`
  - `tenacity`
- Azure CLI installed and logged in with `az databricks` extension configured, if you want automatic workspace URL detection.
- Databricks Bearer token with sufficient API access privileges.
- Access to the target Databricks workspace.

---

## Configuration

Create a `config.json` file containing your Databricks API token in the following format:

~~~json
{
  "token": "<YOUR_DATABRICKS_BEARER_TOKEN>"
}
~~~

This section outlines the key fields extracted from the Databricks workspace JSON summary.

## 1. Clusters
- **clusters**: Object containing cluster details (currently empty in this summary).

---

## 2. SQL Warehouses
Each SQL warehouse includes:
- `name` : Name of the SQL warehouse  
- `state` : Current state (e.g., STOPPED, RUNNING)  
- `cluster_size` : Size of the cluster (e.g., Small, Medium, Large)  

---

## 3. Unity Catalog Tables
Each table entry contains:
- `catalog` : Catalog name  
- `schema` : Schema name  
- `table` : Table name  
- `table_type` : Type of the table  
  - Examples: `MANAGED`, `MATERIALIZED_VIEW`, `STREAMING_TABLE`

---

## 4. Jobs
Each job includes:
- `job_id` : Unique job identifier  
- `name` : Job name  
- `runs` : List of job runs, each containing:  
  - `run_id` : Unique run identifier  
  - `state` : Run state (e.g., TERMINATED)  
  - `result_state` : Result of the run (e.g., SUCCESS)  
  - `start_time` : Run start timestamp  
  - `end_time` : Run end timestamp  

---

## 5. Notebooks
- path
- default_language
- embedded_languages
- other_magics"

## Next Version (Upcoming Enhancements)

In the next release, we plan to enhance the tool with:

- **Playground, Experiments, and Models**
- **Pipelines**
- **Query History**
- **Feature Store**
- **Expanded table metadata** with detailed column-level information.
- **Improved robustness and error handling**

---
