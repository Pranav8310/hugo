+++
date = '2026-04-27T13:11:43+05:30'
draft = false
title = 'foo'
tags = []
categories = ['general']
+++

# Spark GCS Integration Guide

This guide explains how to write data from Apache Spark to Google Cloud Storage (GCS) buckets.

## Prerequisites Setup

### 1. Create a GCP Account
- Create a free tier GCP account using the [GCP Free Trial link](https://console.cloud.google.com/freetrial?facet_utm_source=pmax&facet_utm_campaign=Cloud-SS-DR-GCP-1713664-GCP-DR-APAC-IN-en-PMAX-Display-PMAX-Prospecting-GenericCloud&facet_utm_medium=display&facet_url=https%3A%2F%2Fdocs.cloud.google.com%2Ffree%2Fdocs%2Ffree-cloud-features)

### 2. Create a Service Account (for Authentication)
To enable Spark to write to GCS, you need to create a service account with appropriate permissions:

1. Navigate to **IAM & Admin** > **Service Accounts** in the GCP Console
2. Click **Create Service Account**
3. Provide a descriptive name (e.g., `spark-gcs-writer`)
4. Keep other settings as default and click **Create**
5. In the service account details, go to the **Keys** section
6. Click **Add Key** > **Create New Key**
7. Select **JSON** format and click **Create**
8. Download the JSON key file - this file contains credentials for authentication

**Key File Purpose**: This JSON key file contains the service account credentials that allow Spark to authenticate with GCP and access GCS buckets. Keep this file secure and never commit it to version control.

### 3. Create a GCS Bucket
1. Enable billing for your GCP project (required for GCS usage)
2. Navigate to **Cloud Storage** > **Buckets**
3. Click **Create Bucket**
4. Provide a unique bucket name (e.g., `write_gcs`)
5. Keep other settings as default and click **Create**

## Spark Code Implementation

### Why These Configs Are Needed
Spark uses the Hadoop FileSystem API internally. Since Hadoop doesn’t natively understand GCP/GCS, we need to:
1. Provide the GCS connector library (JAR file)
2. Configure Hadoop to use Google’s GCS implementation
3. Set up authentication using the service account key

### Python Code Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("write_gcs") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", 
            "/tmp/proj/keys/stellar-operand-384014-3353bf8d6e18.json") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()

df = spark.read.csv("/tmp/proj/data/train.csv")

df.write.mode("overwrite").parquet("gs://write_gcs/test/")
```

### Configuration Explanation

| Configuration | Purpose |
|--------------|---------|
| `spark.hadoop.google.cloud.auth.service.account.enable` | Enables service account-based authentication for GCS access |
| `spark.hadoop.google.cloud.auth.service.account.json.keyfile` | Specifies the path to the downloaded JSON key file containing service account credentials |
| `spark.hadoop.fs.gs.impl` | Tells Hadoop to use Google’s `GoogleHadoopFileSystem` implementation for handling `gs://` URIs |
| `spark.hadoop.fs.AbstractFileSystem.gs.impl` | Configures the abstract filesystem layer to use Google’s filesystem implementation for GCS |

## Required Dependencies

### Download the GCS Connector JAR

**JAR File Purpose**: The GCS connector JAR (`gcs-connector-hadoop3-2.2.5-shaded.jar`) is a library that provides the implementation for Spark/Hadoop to interact with Google Cloud Storage. The "shaded" version includes all necessary dependencies bundled together, avoiding dependency conflicts.

```bash
wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.5/gcs-connector-hadoop3-2.2.5-shaded.jar
```

**Key Features of the Shaded JAR**:
- Contains the GCS connector implementation
- Includes all transitive dependencies (shaded/relocated to avoid conflicts)
- Compatible with Hadoop 3.x
- Version 2.2.5 supports the latest GCS features and bug fixes

## Running the Spark Job

Submit your Spark job with the GCS connector JAR on the classpath:

```bash
spark-submit \
    --driver-class-path gcs-connector-hadoop3-2.2.5-shaded.jar \
    --conf spark.executor.extraClassPath=gcs-connector-hadoop3-2.2.5-shaded.jar \
    write_gcs.py
```

### Spark Submit Parameters Explanation

| Parameter | Purpose |
|-----------|---------|
| `--driver-class-path` | Adds the GCS connector JAR to the driver’s classpath so the driver can access GCS |
| `--conf spark.executor.extraClassPath` | Adds the GCS connector JAR to all executor classpaths so executors can read/write GCS data |
| `write_gcs.py` | Your Python script containing the Spark code |

**Note**: Both driver and executors need the JAR because:
- The **driver** needs it to resolve `gs://` paths and plan the job
- The **executors** need it to actually read from or write data to GCS

## Troubleshooting Tips

1. **Authentication Errors**: Ensure the service account has the necessary IAM roles (e.g., `Storage Object Admin`)
2. **Class Not Found**: Verify the JAR file path is correct in the spark-submit command
3. **Bucket Access Denied**: Check that billing is enabled and the bucket name is correct in your code


