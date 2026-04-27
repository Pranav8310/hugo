+++
date = '2026-04-27T13:11:43+05:30'
draft = false
title = 'Myth "DuckDb is only for small data"'
tags = ['Spark', 'DuckDb']
categories = ['general']
+++

Here’s the thing. Everyone told me DuckDB was just for toy datasets. You know, the stuff that fits in a few gigabytes. Maybe good for prototyping. But real data? Nah, you need Spark for that. I didn’t believe it.

### The Rumor That Started Everything

A few months back, I kept hearing the same thing: “DuckDB is only for small data — megabytes, maybe gigabytes at best.” People said it like it was gospel.

Then I stumbled on a blog: [link](https://blog.dataexpert.io/p/i-processed-1-tb-with-duckdb-in-30)

Wait. They processed 1TB in 30 seconds? I had to test this myself.

### My Setup (Spoiler: Not Impressive)

I wanted to compare two approaches:
- Distributed computing (PySpark)
- Single-node computing (DuckDB)

Now, I didn’t have access to cloud clusters or fancy hardware. Just my M1 MacBook. So I started small. 10GB of parquet data. Not massive. But enough to prove a point.

### Generating the Test Data

I created a billion-row dataset using DuckDB itself. Here’s the code:

```python
import duckdb
import os

target_rows = 1_000_000_000  # 1 Billion rows
output_file = '/Downloads/duckdb/raw_data/*.parquet'

conn = duckdb.connect(database=':memory:')

try:
    print(f"Generating {target_rows:,} rows...")
    
    generate_sql = f"""
    CREATE TABLE IF NOT EXISTS large_table AS
    SELECT 
        i as id,
        'User_' || (i % 10000) as user_name,
        (i % 100) as value,
        random()::VARCHAR as random_string,
        now() - INTERVAL (i::INT % 1000) DAYS as transaction_date
    FROM range({target_rows}) tbl(i);
    """
    
    conn.execute(generate_sql)
    
    copy_sql = f"""
    COPY large_table TO '{output_file}' 
    (FORMAT 'PARQUET', CODEC 'ZSTD', ROW_GROUP_SIZE 100000);
    """
    
    conn.execute(copy_sql)
    print("Data exported successfully!")
    
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()

file_size_gb = os.path.getsize(output_file) / (1024**3)
print(f"File size: {file_size_gb:.2f} GB")
```

Eventually, I had my 10GB parquet file ready. Time to benchmark.

### The DuckDB Approach

I wrote a simple script to query the data. Run it 5 times. Average the results.

```python
import duckdb
import time

file_path = '/Downloads/duckdb/raw_data/test.parquet'
iterations = 5
conn = duckdb.connect(':memory:')
sql = f"""
    SELECT * FROM read_parquet('{file_path}')
    ORDER BY 1 DESC 
    LIMIT 10
"""
run_times = []
for i in range(1, iterations + 1):
    start_time = time.perf_counter()
    result_df = conn.execute(sql).df()
    end_time = time.perf_counter()
    
    duration = end_time - start_time
    run_times.append(duration)
    
    print(f"Run {i}: {duration:.4f} seconds")
avg_time = sum(run_times) / iterations
print(f"\nAverage time: {avg_time:.4f} seconds")
print(f"Fastest run: {min(run_times):.4f} seconds")
conn.close()
```

Nothing fancy. Just raw speed.

### The PySpark Approach

Same query. Same data.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("benchmark").getOrCreate()
df = spark.read.format('parquet').load('/Downloads/duckdb/raw_data/test.parquet')
df.createOrReplaceTempView("foo")
result = spark.sql("""
    SELECT * FROM foo
    ORDER BY 1 DESC 
    LIMIT 10
""")
result.show()
```

Standard Spark stuff. Let’s see how it performs.

### The Results Blew My Mind

DuckDB: 2.58 seconds

PySpark: 8 seconds

Wait, what? DuckDB was more than 3x faster than Spark on my local machine. For the same exact query. Same data. Same hardware.

### Why Does This Matter?

Here’s what I learned:

- ***No cluster needed:*** DuckDB runs on a single machine. No setup headaches. 
- ***Blazing fast:*** It’s optimized for analytical queries out of the box.
- ***Simple code:*** Look at that DuckDB script. So clean.
- ***Resource efficient:*** My laptop didn’t catch fire.

The best part? ***1TB in under 30 seconds using DuckDB.*** Not 10GB. 1TB. Just WoWWWWW.

### What I’m Taking Away

Distributed computing has its place. But for most analytical workloads? DuckDB might be all you need.

No Spark clusters. No YAML configs. No waiting for executors to spin up. Just fast, simple SQL on your local machine.

Does it make sense?

Next time someone tells you DuckDB is “only for small data,” send them this. Let the results speak for themselves.
