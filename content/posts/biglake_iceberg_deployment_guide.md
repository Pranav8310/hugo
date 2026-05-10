+++
date = '2026-05-10T16:50:00+05:30'
draft = false
title = 'Complete Guide: Creating BigLake Iceberg Tables with Spark and GitHub Actions'
tags = ['BigQuery', 'BigLake', 'Iceberg', 'Spark', 'GCP', 'GitHub Actions']
categories = ['data-engineering', 'cloud']
+++

This comprehensive guide walks you through creating BigLake Iceberg tables using Apache Spark, deploying them with GitHub Actions, and linking metadata for querying in BigQuery.

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Prerequisites Setup](#prerequisites-setup)
3. [Part 1: Local Spark to GCS with Iceberg](#part-1-local-spark-to-gcs-with-iceberg)
4. [Part 2: BigQuery Connections Setup](#part-2-bigquery-connections-setup)
5. [Part 3: Creating BigLake External Tables](#part-3-creating-biglake-external-tables)
6. [Part 4: GitHub Actions Deployment](#part-4-github-actions-deployment)
7. [Complete Workflow](#complete-workflow)

---

## Architecture Overview

```
┌─────────────────────┐
│  Local Spark Job    │
│  (foo.py)           │
│  Writes Iceberg     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────┐
│  Google Cloud Storage (GCS)         │
│  gs://write_gcs/iceberg-warehouse/  │
│    └── atomic_orders/orders/        │
│        ├── data/                    │
│        │   └── *.parquet            │
│        └── metadata/                │
│            ├── v1.metadata.json ◄───┼─── Iceberg Metadata
│            ├── v2.metadata.json     │
│            └── v5.metadata.json     │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  BigQuery Connection                │
│  (my-biglake-connection)            │
│  Type: Cloud Resource (Lakehouse)   │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  GitHub Repository                  │
│  ├── ddl/                           │
│  │   └── creat_iceberg_table.sql   │◄─── DDL Definition
│  └── .github/workflows/             │
│      └── deploy-ddl.yml             │
└─────────────┬───────────────────────┘
              │ GitHub Actions
              ▼
┌─────────────────────────────────────┐
│  BigQuery BigLake External Table    │
│  stellar-operand-384014             │
│    .atomic_orders.orders            │
│                                     │
│  Reads from: v5.metadata.json       │
└─────────────────────────────────────┘
```

---

## Prerequisites Setup

### 1. Create a GCP Account
- Create a free tier GCP account using the [GCP Free Trial link](https://console.cloud.google.com/freetrial)
- Enable billing for your project

### 2. Create a Service Account (for Spark Authentication)

1. Navigate to **IAM & Admin** > **Service Accounts** in the GCP Console
2. Click **Create Service Account**
3. Provide a name (e.g., `spark-iceberg-writer`)
4. Grant the following roles:
   - **Storage Object Admin** - to write to GCS
   - **BigQuery Data Editor** - to manage BigLake tables
5. Click **Create Key** > **JSON** format
6. Download and save the JSON key file securely

**Security Note**: Keep this file secure. Store it in a safe location (e.g., `/Users/YOUR_USER/personal/proj/keys/`) and never commit it to version control.

### 3. Create GCS Buckets

```bash
# Source bucket for Iceberg data
gsutil mb -p stellar-operand-384014 -l us gs://write_gcs/

# Verify bucket creation
gsutil ls
```

### 4. Enable Required GCP APIs

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable bigqueryconnection.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable iam.googleapis.com
```

---

## Part 1: Local Spark to GCS with Iceberg

### 1.1 Download Required JARs

Download the necessary JAR files for Iceberg and GCS connectivity:

```bash
# Create jars directory
mkdir -p ~/personal/proj/jars
cd ~/personal/proj/jars

# Download Iceberg Spark Runtime
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar

# Download GCS Connector (shaded version)
wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.5/gcs-connector-hadoop3-2.2.5-shaded.jar
```

**JAR Purposes**:
- **Iceberg Spark Runtime**: Enables Spark to read/write Iceberg table format with ACID transactions and time travel
- **GCS Connector**: Allows Hadoop/Spark to interact with Google Cloud Storage using `gs://` URIs

### 1.2 Create Spark Python Script

Create `foo.py`:

```python
from pyspark.sql import SparkSession

key_path = "/Users/A200315862/personal/proj/keys/stellar-operand-384014-3353bf8d6e18.json"

spark = SparkSession.builder \
    .appName("write_iceberg_to_gcs") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.dev.type", "hadoop") \
    .config("spark.sql.catalog.dev.warehouse", "gs://write_gcs/iceberg-warehouse") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path) \
    .getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/Users/A200315862/personal/proj/data/train.csv")

df.writeTo("dev.atomic_orders.orders").using("iceberg").createOrReplace()
```

### 1.3 Configuration Explanation

| Configuration | Purpose |
|--------------|---------|
| `spark.sql.extensions` | Registers Iceberg's SQL extensions for CREATE TABLE, ALTER TABLE, etc. |
| `spark.sql.catalog.dev` | Defines a catalog named "dev" using Iceberg's SparkCatalog |
| `spark.sql.catalog.dev.type` | Uses Hadoop catalog type (stores metadata in file system) |
| `spark.sql.catalog.dev.warehouse` | Root location in GCS for all Iceberg tables |
| `fs.gs.impl` | Hadoop FileSystem implementation for `gs://` URIs |
| `fs.AbstractFileSystem.gs.impl` | Abstract FileSystem implementation for GCS |
| `spark.hadoop.google.cloud.auth.service.account.enable` | Enables service account authentication |
| `spark.hadoop.google.cloud.auth.service.account.json.keyfile` | Path to service account JSON key |

### 1.4 Run Spark Job

```bash
spark-submit \
    --jars /path_to_your_jar_file/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/path_to_your_jar_file/gcs-connector-hadoop3-2.2.5-shaded.jar \
    --driver-class-path /path_to_your_jar_file/gcs-connector-hadoop3-2.2.5-shaded.jar:/path_to_your_jar_file/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    --conf spark.executor.extraClassPath=/path_to_your_jar_file/gcs-connector-hadoop3-2.2.5-shaded.jar:/path_to_your_jar_file/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    foo.py
```

### 1.5 Verify Iceberg Table Creation

After successful execution, verify the Iceberg structure in GCS:

```bash
gsutil ls -r gs://write_gcs/iceberg-warehouse/atomic_orders/orders/
```

**Expected Output**:
```
gs://write_gcs/iceberg-warehouse/atomic_orders/orders/data/
gs://write_gcs/iceberg-warehouse/atomic_orders/orders/data/00000-0-*.parquet
gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/
gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v1.metadata.json
gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/*.avro
gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/snap-*.avro
```

**Key Components**:
- **data/**: Contains Parquet files with actual table data
- **metadata/v1.metadata.json**: First version of Iceberg metadata (schema, partitioning, snapshots)
- **metadata/*.avro**: Manifest files tracking data files and statistics

### 1.6 Understanding Iceberg Metadata Versioning

Each write operation creates a new metadata version:

```bash
# View all metadata versions
gsutil ls gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v*.json
```

**Important**: Always use the **latest version** (highest number) when creating BigLake external tables.

---

## Part 2: BigQuery Connections Setup

BigQuery connections enable secure access to external data sources. We'll create a **Cloud Resource (Lakehouse)** connection for Iceberg tables.

### 2.1 Understanding BigQuery Connection Types

BigQuery supports multiple connection types:

| Connection Type | Use Case | Example |
|----------------|----------|---------|
| **Cloud Resource (Lakehouse)** | Access Iceberg/Delta/Hudi tables in GCS | Our use case |
| **Cloud Resource (Spanner)** | Query Cloud Spanner databases | Cross-database queries |
| **Vertex AI remote models** | Use AI/ML models in SQL | ML predictions |
| **Remote functions** | Call Cloud Functions/Run from SQL | Custom logic |

### 2.2 Create BigLake Connection

#### Using GCP Console (Recommended for First Time)

1. Navigate to **BigQuery** in GCP Console
2. Click **Add** > **Connections to external data sources**
3. Select **Connection type**: **Cloud Resource**
4. Choose **Connection location**: `us` (or your preferred region)
5. Enter **Connection ID**: `my-biglake-connection`
6. Click **Create Connection**
7. **Important**: Copy the **Service Account** email shown (format: `bqcx-PROJECT-NUMBER@gcp-sa-bigquery-condel.iam.gserviceaccount.com`)

### 2.3 Grant Connection Permissions to GCS

The BigQuery connection service account needs permission to read from your GCS bucket:

```bash
# Replace with your connection service account from previous step
export CONNECTION_SA="bqcx-123456789@gcp-sa-bigquery-condel.iam.gserviceaccount.com"

# Grant Storage Object Viewer role
gsutil iam ch serviceAccount:${CONNECTION_SA}:objectViewer gs://write_gcs
```

**Why This Is Needed**: The BigQuery connection uses this service account to access Iceberg metadata and data files in GCS when querying the table.


## Part 3: Creating BigLake External Tables

### 3.1 Understanding External vs Managed Tables

| Feature | Managed Iceberg Table | External Iceberg Table (BigLake) |
|---------|----------------------|----------------------------------|
| **Data Storage** | BigQuery-managed | Customer GCS bucket |
| **Metadata Management** | BigQuery | Manual (you manage) |
| **Cost** | Storage + compute | Compute only (cheaper) |
| **Use Case** | Full BigQuery features | Existing Iceberg tables |
| **Schema Evolution** | Automatic | Manual metadata update |

**For our scenario**: We use **External Tables (BigLake)** because we're creating Iceberg tables with Spark and want to query them in BigQuery.

### 3.2 Linking Iceberg Metadata (v1.metadata.json)

The metadata JSON file is the **single source of truth** for Iceberg tables. It contains:
- Table schema (column names, types)
- Partitioning spec
- Snapshot history (versions)
- Manifest list locations (points to data files)

**Key Concept**: BigLake external tables **don't store data** - they read the metadata file to understand:
1. Where data files are located
2. What the schema is
3. Which snapshots to query

### 3.3 Create DDL File

Create `creat_iceberg_table.sql`:

```sql
CREATE OR REPLACE EXTERNAL TABLE `stellar-operand-384014.atomic_orders.orders`
WITH CONNECTION `us.my-biglake-connection`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v5.metadata.json']
);
```

**DDL Explanation**:

| Component | Purpose |
|-----------|---------|
| `CREATE OR REPLACE EXTERNAL TABLE` | Creates a BigLake external table (can update if exists) |
| `stellar-operand-384014.atomic_orders.orders` | Fully qualified table name: `PROJECT.DATASET.TABLE` |
| `WITH CONNECTION` | References the BigLake connection for authentication |
| `us.my-biglake-connection` | Connection in `REGION.CONNECTION_NAME` format |
| `format = 'ICEBERG'` | Specifies Iceberg table format |
| `uris = [...]` | Points to the Iceberg metadata JSON file |

**Critical**: The URI must point to the **latest metadata version** (e.g., `v5.metadata.json`). If you run more Spark jobs, you'll need to update this to `v6.metadata.json`, `v7.metadata.json`, etc.

### 3.4 Execute DDL Manually (Testing)

```bash
# Run the DDL
bq query --use_legacy_sql=false < creat_iceberg_table.sql

# Verify table creation
bq show stellar-operand-384014:atomic_orders.orders

# Query the table
bq query --use_legacy_sql=false \
  "SELECT * FROM \`stellar-operand-384014.atomic_orders.orders\` LIMIT 10"
```

### 3.5 Updating Metadata After New Spark Runs

After running your Spark job again:

1. **Check the new metadata version**:
   ```bash
   gsutil ls gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v*.json
   ```

2. **Update the DDL** with the new version:
   ```sql
   CREATE OR REPLACE EXTERNAL TABLE `stellar-operand-384014.atomic_orders.orders`
   WITH CONNECTION `us.my-biglake-connection`
   OPTIONS (
     format = 'ICEBERG',
     uris = ['gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v6.metadata.json']
   );
   ```

3. **Re-run the DDL** to update the BigLake table.

**Automation Tip**: This manual update process can be automated using GitHub Actions (covered in Part 4).

---

## Part 4: GitHub Actions Deployment

Automate DDL deployment using GitHub Actions with Workload Identity Federation (no service account keys needed!).

### 4.1 Repository Setup

```bash
# Create repository structure
mkdir -p spark-biglake-pipeline
cd spark-biglake-pipeline

# Create directories
mkdir -p ddl
mkdir -p .github/workflows

# Initialize git
git init
```

### 4.2 Add DDL File

Save your DDL in `ddl/creat_iceberg_table.sql`:

```sql
CREATE OR REPLACE EXTERNAL TABLE `stellar-operand-384014.atomic_orders.orders`
WITH CONNECTION `us.my-biglake-connection`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v5.metadata.json']
);
```

### 4.3 Configure Workload Identity Federation

Workload Identity Federation allows GitHub Actions to authenticate to GCP without storing service account keys.

#### Step 1: Set Variables

```bash
export PROJECT_ID="stellar-operand-384014"
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
export POOL_NAME="github-actions-pool"
export PROVIDER_NAME="github-provider"
export SA_NAME="github-ddl-deployer"
export REPO_OWNER="your-github-username"
export REPO_NAME="spark-biglake-pipeline"
```

#### Step 2: Create Service Account

```bash
# Create service account
gcloud iam service-accounts create $SA_NAME \
  --display-name="GitHub Actions DDL Deployer" \
  --project=$PROJECT_ID

# Grant BigQuery Admin role (to create/update tables)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"

# Grant BigQuery Connection Admin (to use connections)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.connectionAdmin"
```

#### Step 3: Create Workload Identity Pool

```bash
# Create pool
gcloud iam workload-identity-pools create $POOL_NAME \
  --location="global" \
  --display-name="GitHub Actions Pool" \
  --project=$PROJECT_ID

# Verify creation
gcloud iam workload-identity-pools describe $POOL_NAME \
  --location="global" \
  --project=$PROJECT_ID
```

#### Step 4: Create OIDC Provider

```bash
gcloud iam workload-identity-pools providers create-oidc $PROVIDER_NAME \
  --location="global" \
  --workload-identity-pool=$POOL_NAME \
  --display-name="GitHub Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository_owner=='${REPO_OWNER}'" \
  --project=$PROJECT_ID
```

**Security Note**: The `attribute-condition` restricts access to only your GitHub account.

#### Step 5: Allow GitHub to Impersonate Service Account

```bash
gcloud iam service-accounts add-iam-policy-binding \
  "${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_NAME}/attribute.repository/${REPO_OWNER}/${REPO_NAME}" \
  --project=$PROJECT_ID
```

#### Step 6: Get WIF Provider Name

```bash
gcloud iam workload-identity-pools providers describe $PROVIDER_NAME \
  --location="global" \
  --workload-identity-pool=$POOL_NAME \
  --project=$PROJECT_ID \
  --format="value(name)"
```

**Save this output** - you'll need it for GitHub secrets.

**Example output**:
```
projects/123456789/locations/global/workloadIdentityPools/github-actions-pool/providers/github-provider
```

### 4.4 Configure GitHub Secrets

Go to your GitHub repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these secrets:

| Secret Name | Value | Example |
|-------------|-------|---------|
| `GCP_PROJECT_ID` | Your GCP project ID | `stellar-operand-384014` |
| `BQ_DATASET` | BigQuery dataset name | `atomic_orders` |
| `WIF_PROVIDER` | Workload Identity Provider (from Step 6) | `projects/123456789/locations/global/...` |
| `GCP_SERVICE_ACCOUNT` | Service account email | `github-ddl-deployer@stellar-operand-384014.iam.gserviceaccount.com` |

### 4.5 Create GitHub Actions Workflow

Create `.github/workflows/deploy-ddl.yml`:

```yaml
name: Deploy BigQuery DDL

on:
  push:
    branches:
      - main
    paths:
      - 'ddl/**'
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

jobs:
  deploy-ddl:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}

      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2

      - name: Deploy DDL files
        run: |
          for sql_file in ddl/*.sql; do
            echo "Executing $sql_file..."
            bq query --use_legacy_sql=false < "$sql_file"
            if [ $? -eq 0 ]; then
              echo " Successfully executed $sql_file"
            else
              echo " Failed to execute $sql_file"
              exit 1
            fi
          done

      - name: Verify table creation
        run: |
          bq show ${{ secrets.GCP_PROJECT_ID }}:${{ secrets.BQ_DATASET }}.orders
```

**Workflow Explanation**:

| Section | Purpose |
|---------|---------|
| `on: push` | Triggers on push to main branch when DDL files change |
| `workflow_dispatch` | Allows manual triggering from GitHub UI |
| `permissions: id-token: write` | Required for Workload Identity Federation |
| `google-github-actions/auth@v2` | Authenticates using WIF (no keys!) |
| `google-github-actions/setup-gcloud@v2` | Installs gcloud CLI with bq command |
| `Deploy DDL files` | Executes all SQL files in ddl/ directory |
| `Verify table creation` | Confirms table exists after deployment |

### 4.6 Push to GitHub

```bash
# Add all files
git add .

# Commit
git commit -m "Initial setup: DDL and GitHub Actions workflow"

# Add remote (replace with your repo URL)
git remote add origin https://github.com/your-username/spark-biglake-pipeline.git

# Push to main branch
git push -u origin main
```

### 4.7 Monitor Deployment

1. Go to your GitHub repository
2. Click **Actions** tab
3. You should see the "Deploy BigQuery DDL" workflow running
4. Click on the workflow run to see logs


### 4.8 Updating Metadata Versions via GitHub

When you run new Spark jobs and need to update to v6.metadata.json:

1. **Update the DDL file** locally:
   ```sql
   CREATE OR REPLACE EXTERNAL TABLE `stellar-operand-384014.atomic_orders.orders`
   WITH CONNECTION `us.my-biglake-connection`
   OPTIONS (
     format = 'ICEBERG',
     uris = ['gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v6.metadata.json']
   );
   ```

2. **Commit and push**:
   ```bash
   git add ddl/creat_iceberg_table.sql
   git commit -m "Update to v6 metadata"
   git push origin main
   ```

3. **GitHub Actions automatically deploys** the updated DDL.

---

## Complete Workflow

Here's the end-to-end process:

### Initial Setup (One-Time)

1. Create GCP project and enable APIs
2. Create service account for Spark
3. Create GCS bucket
4. Download Iceberg and GCS connector JARs
5. Create BigQuery dataset
6. Create BigLake connection
7. Grant connection permissions to GCS
8. Set up Workload Identity Federation
9. Configure GitHub repository with secrets
10. Create GitHub Actions workflow

### Regular Development Workflow

#### Step 1: Write Data with Spark

```bash
# Run Spark job
spark-submit \
    --jars ~/personal/proj/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,~/personal/proj/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
    --driver-class-path ~/personal/proj/jars/gcs-connector-hadoop3-2.2.5-shaded.jar:~/personal/proj/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    --conf spark.executor.extraClassPath=~/personal/proj/jars/gcs-connector-hadoop3-2.2.5-shaded.jar:~/personal/proj/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    foo.py
```

#### Step 2: Check Metadata Version

```bash
gsutil ls gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v*.json
```

#### Step 3: Update DDL (if new version)

If a new metadata version was created:

1. Update `ddl/creat_iceberg_table.sql` with the new version number
2. Commit and push to GitHub
3. GitHub Actions deploys automatically

#### Step 4: Query in BigQuery

```sql
-- Query the table
SELECT * FROM `stellar-operand-384014.atomic_orders.orders` LIMIT 100;

-- Check row count
SELECT COUNT(*) FROM `stellar-operand-384014.atomic_orders.orders`;

-- Time travel query (if multiple snapshots)
SELECT * FROM `stellar-operand-384014.atomic_orders.orders`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

---

## Key Differences: Iceberg vs Traditional Tables

### Why No `_SUCCESS` File?

Traditional Spark writes create a `_SUCCESS` marker:
```
gs://bucket/output/
├── part-00000.parquet
├── part-00001.parquet
└── _SUCCESS  ← Marker file
```

**Iceberg doesn't need this** because:
- Iceberg uses **ACID transactions** with metadata versioning
- The **committed metadata file** (`v5.metadata.json`) proves the write succeeded
- Each metadata version is atomic - either fully committed or not visible

### Metadata Version History

```
v1.metadata.json  →  Initial table creation
v2.metadata.json  →  First append operation
v3.metadata.json  →  Second append
v4.metadata.json  →  Compaction operation
v5.metadata.json  →  Latest snapshot (current state)
```

**Time Travel**: You can query any historical version by pointing to older metadata files (if needed).

---

## Troubleshooting

### Issue 1: "Permission Denied" when querying BigLake table

**Cause**: BigQuery connection service account doesn't have GCS access.

**Solution**:
```bash
gsutil iam ch serviceAccount:bqcx-xxx@gcp-sa-bigquery-condel.iam.gserviceaccount.com:objectViewer gs://write_gcs
```

### Issue 2: "Metadata file not found"

**Cause**: Wrong metadata version in DDL or file doesn't exist.

**Solution**:
```bash
# List all versions
gsutil ls gs://write_gcs/iceberg-warehouse/atomic_orders/orders/metadata/v*.json

# Update DDL with the latest version
```

### Issue 3: GitHub Actions fails with authentication error

**Cause**: Workload Identity Federation not configured correctly.

**Solution**:
1. Verify the WIF provider name in GitHub secrets
2. Check service account has `roles/iam.workloadIdentityUser`
3. Ensure `permissions: id-token: write` is set in workflow

### Issue 4: Schema mismatch errors

**Cause**: DDL schema doesn't match Iceberg table schema.

**Solution**: BigLake external tables automatically read schema from Iceberg metadata - no need to specify schema in DDL.

---

## Summary

This guide covered:

1. **Writing Iceberg tables from Spark** to GCS with proper configurations
2. **Creating BigQuery connections** (Cloud Resource - Lakehouse type)
3. **Linking Iceberg metadata** (`v1.metadata.json`) with BigLake external tables
4. **Automating DDL deployment** using GitHub Actions with Workload Identity Federation
5. **Understanding metadata versioning** and updating tables after new Spark runs
