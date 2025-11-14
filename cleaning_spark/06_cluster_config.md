# Cluster Configuration for Efficient JSON Ingestion and Transformation

*Context: Loading and transforming variable-format JSON files in Databricks via VS Code*

---
## Checking
```python
# Name of the Spark application instance
app_name = spark.sparkContext.getConf().get("spark.app.name")

# Driver TCP port
driver_port = spark.sparkContext.getConf().get("spark.driver.port")

# Number of join partitions
num_partitions = spark.conf.get('spark.sql.shuffle.partitions')

# Show the results
print("Name: %s" % app_name)
print("Driver TCP port: %s" % driver_port)
print("Number of partitions: %s" % num_partitions)
```
## Setting
```python
# Store the number of partitions in variable
before = departures_df.rdd.getNumPartitions()

# Configure Spark to use 500 partitions
spark.conf.set('spark.sql.shuffle.partitions', 500)

# Recreate the DataFrame using the departures data file
departures_df = spark.read.csv('departures.txt.gz').distinct()

# Print the number of partitions for each instance
print("Partition count before change: %d" % before)
print("Partition count after change: %d" % departures_df.rdd.getNumPartitions())
```
## 1. Why Cluster Configuration Matters

Even with optimized code, **poor cluster setup** can make your Spark job slow or unreliable.
Cluster configuration determines:

* How Spark **distributes tasks** across worker nodes.
* How much **memory and CPU** is available per executor.
* How many files and partitions can be processed **in parallel**.

In your workflow — where JSON files vary in format and size — a properly tuned cluster ensures that:

* Schema inference and parsing happen quickly.
* Transformations don’t spill to disk.
* Parallel import and writing to Delta/Parquet tables are balanced.

---

## 2. Databricks Cluster Basics

A Databricks cluster has:

* **Driver node:** coordinates tasks and holds metadata (e.g., schema inference).
* **Worker nodes:** perform actual reading, transformation, and writing of data.

Each node runs multiple **executors**, and each executor runs multiple **tasks** (parallel units of computation).

---

## 3. Key Parameters That Affect Performance

### (a) **Number of Worker Nodes**

More workers = more parallelism.
Start small (e.g., 1–2 workers) and scale up depending on data volume.

| Scenario                                 | Recommended Workers |
| ---------------------------------------- | ------------------- |
| Testing / Schema exploration             | 1 small worker      |
| Production load (hundreds of JSON files) | 4–8 workers         |
| Very large datasets (multi-GB)           | 8–16 workers        |

In Databricks, you can configure this under:
**Compute → Cluster → Worker Type and Count**

---

### (b) **Autoscaling**

Enable **autoscaling** to adjust cluster size based on workload.
It prevents overpaying during idle periods while handling load spikes during heavy imports.

```bash
Min workers: 2
Max workers: 8
```

Databricks will scale up during heavy ingestion, then scale down automatically.

---

### (c) **Worker Type (Memory vs. Compute Optimized)**

Choose based on workload characteristics:

| Worker Type           | Best For                                      | Example                   |
| --------------------- | --------------------------------------------- | ------------------------- |
| **Memory-optimized**  | Large JSONs, schema inference, nested objects | `r5d.xlarge`, `m5d.large` |
| **Compute-optimized** | Many small files, light parsing               | `c5d.xlarge`              |
| **Storage-optimized** | Heavy write workloads (e.g., Delta tables)    | `i3.xlarge`               |

For JSON-heavy ingestion, **memory-optimized** instances are safest.

---

### (d) **Executor Memory and Cores**

Each Spark executor runs tasks in parallel. You can control memory and CPU with Spark configuration parameters.

Typical defaults in Databricks work well, but if you tune manually:

```python
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.executor.cores", "2")
spark.conf.set("spark.driver.memory", "4g")
```

Guidelines:

* At least **4 GB memory per executor** for nested JSON parsing.
* 1–4 cores per executor; more cores can cause GC (garbage collection) overhead.
* Leave some memory headroom for the OS and Databricks runtime.

---

### (e) **Dynamic Allocation**

Spark can dynamically add or remove executors based on stage needs.

Enable it:

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "10")
```

This complements Databricks autoscaling — Spark manages executors inside the cluster while Databricks manages nodes.

---

## 4. Key Configurations for JSON Workloads

These Spark settings can greatly affect performance during JSON ingestion:

| Configuration                       | Description                        | Recommended Value               |
| ----------------------------------- | ---------------------------------- | ------------------------------- |
| `spark.sql.files.maxPartitionBytes` | Max data per partition             | `128MB`                         |
| `spark.sql.files.openCostInBytes`   | Cost threshold for splitting files | `4MB`                           |
| `spark.sql.shuffle.partitions`      | Partitions after shuffles          | `200` (tune based on data size) |
| `spark.executor.heartbeatInterval`  | Avoid executor timeouts            | `60s`                           |
| `spark.network.timeout`             | Stability for long imports         | `300s`                          |

Example setup:

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

---

## 5. Monitoring Cluster Utilization

Use **Databricks UI → Spark UI → Stages / Executors** to observe:

* **Active tasks:** if too few, consider adding workers or splitting files.
* **GC time:** if high, increase memory or reduce executor cores.
* **Shuffle read/write:** large values mean repartitioning or joins need optimization.

You can also check:

```python
df.rdd.getNumPartitions()
```

to verify data distribution across executors.

---

## 6. Example Configuration Snippet

Below is an example configuration setup for a moderate JSON workload (hundreds of small to mid-sized files):

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("JSON_Ingestion_Workflow")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "2")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.files.maxPartitionBytes", "128MB")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", "2")
    .config("spark.dynamicAllocation.maxExecutors", "10")
    .getOrCreate())
```

---

## 7. Summary of Cluster Best Practices

| Goal                    | Setting / Action                                          |
| ----------------------- | --------------------------------------------------------- |
| Maximize parallelism    | Use multiple workers; split large files                   |
| Prevent memory errors   | Use memory-optimized nodes; sufficient executor memory    |
| Handle uneven load      | Enable autoscaling + dynamic allocation                   |
| Control shuffle cost    | Tune `spark.sql.shuffle.partitions`                       |
| Improve fault tolerance | Increase `spark.network.timeout` for long-running imports |
| Monitor jobs            | Use Databricks Spark UI (Stages, Executors tabs)          |

