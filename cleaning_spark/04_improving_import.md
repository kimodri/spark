# Improving Import Performance in PySpark

*Context: JSON ingestion and transformation workflow on Databricks using VS Code*

---

## 1. Understanding the Import Process

When you load JSON data into Spark, the process typically involves:

1. **Reading raw JSON files** into a DataFrame (`spark.read.json()`).
2. **Inferring or applying a schema.**
3. **Converting semi-structured data** into a structured form suitable for transformation and table storage.

Because JSON is flexible and nested, inefficient import practices can lead to slow performance, especially when:

* The JSON files vary in structure.
* Spark infers schemas automatically on large datasets.
* The data is stored on distributed file systems (e.g., S3, ADLS).

---

## 2. Key Principles for Faster Imports

### (a) **Avoid Full Schema Inference on Every Read**

Schema inference forces Spark to scan data before loading it. This doubles I/O and is costly when you have many files.

**Better approach:**

* Infer the schema once, then reuse it.
* Or define the schema manually if known.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Example schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.schema(schema).json("dbfs:/mnt/data/users/*.json")
```

If you cannot define the schema because formats vary, see Section 4 (Handling Varying Formats).

---

### (b) **Combine Small Files Before Reading**

Spark treats each small file as a separate partition, which creates overhead.

**Fix:** Use `spark.read.option("multiLine", True)` or combine small files into fewer, larger files using Databricks utilities or an initial ingestion step.

Example in Databricks:

```python
# Combine small files into one large DataFrame and write back
raw_df = spark.read.json("dbfs:/mnt/raw/json_files/")
raw_df.coalesce(10).write.mode("overwrite").json("dbfs:/mnt/processed/combined/")
```

Then:

```python
df = spark.read.json("dbfs:/mnt/processed/combined/")
```

---

### (c) **Use Column Pruning and Predicate Pushdown**

When you only need certain fields or rows, don’t load everything.

```python
df = spark.read.json("dbfs:/mnt/data/events/")
filtered_df = df.select("event_id", "timestamp", "user_id").filter("event_type = 'click'")
```

Spark reads only the needed columns and rows, minimizing I/O.

---

### (d) **Leverage Parallelism Wisely**

Spark parallelizes file reading across workers. But too few or too many partitions can slow performance.

Check partition count:

```python
df.rdd.getNumPartitions()
```

Adjust partitions:

```python
df = df.repartition(8)   # Increase parallelism if data is large
```

Or coalesce when writing to reduce shuffle cost:

```python
df.coalesce(4).write.mode("overwrite").parquet("dbfs:/mnt/cleaned/events/")
```

---

### (e) **Cache During Iterative Development**

If you’ll reuse the same dataset multiple times (for schema inspection, validation, or transformation), caching avoids reloading.

```python
df.cache()
df.count()  # triggers cache
```

Later:

```python
df.unpersist()
```

---

## 3. Databricks-Specific Optimizations

### (a) **Auto Loader for Incremental JSON Loads**

When you expect new JSON files over time, Databricks’ Auto Loader automatically detects and ingests them efficiently.

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("dbfs:/mnt/raw/json_data/"))
```

It supports schema evolution, file notification, and efficient ingestion with checkpointing.

### (b) **Use Delta Lake for Intermediate Storage**

Instead of re-parsing JSON repeatedly, convert your ingested data into **Delta format** once.

```python
df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze/events")
```

Next time you need it:

```python
df = spark.read.format("delta").load("/mnt/delta/bronze/events")
```

Delta is columnar, optimized for reads, and supports incremental updates.

---

## 4. Handling Varying JSON Structures

When JSON structures vary, schema inference becomes unpredictable. A robust approach involves:

1. Loading JSONs as raw text.
2. Parsing with fallback logic.

```python
from pyspark.sql.functions import from_json, schema_of_json, col

# Load as text to inspect structure dynamically
raw_df = spark.read.text("dbfs:/mnt/raw/json_files/")

# Infer schema from a sample (only once)
sample_json = raw_df.limit(1).collect()[0][0]
schema = schema_of_json(sample_json)

# Parse each JSON using the inferred schema
df = raw_df.withColumn("data", from_json(col("value"), schema)).select("data.*")
```

You can generalize this pattern to infer different schemas per directory or prefix, depending on your source system.

---

## 5. Tips for Large-Scale JSON Ingestion

| Tip                                                                   | Why it helps                                    |
| --------------------------------------------------------------------- | ----------------------------------------------- |
| Use **`spark.sql.files.maxPartitionBytes`** to control file splitting | Avoids loading huge files in one partition      |
| Use **`spark.sql.shuffle.partitions`** wisely                         | Reduces shuffle overhead during transformations |
| Validate JSON with `spark.read.option("mode", "PERMISSIVE")`          | Prevents job failure from corrupt records       |
| Store intermediate data as **Parquet/Delta**                          | Columnar formats load much faster               |
| Profile data sizes early (`df.inputFiles()`)                          | Helps tune cluster size and partitioning        |

---

## 6. Example: End-to-End Efficient JSON Load

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("JSONLoader").getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event", StringType(), True)
])

# Efficient read
df = (spark.read
      .schema(schema)
      .option("mode", "PERMISSIVE")
      .json("dbfs:/mnt/raw/events/*.json"))

# Transformation
cleaned_df = df.filter("event IS NOT NULL")

# Write as Delta
cleaned_df.coalesce(8).write.format("delta").mode("overwrite").save("/mnt/delta/events_cleaned/")
```

---

## 7. Key Takeaways

1. Avoid schema inference on large or inconsistent JSONs.
2. Combine small files to reduce partition overhead.
3. Cache data only when reused often.
4. Use Delta for downstream performance and consistency.
5. For Databricks, Auto Loader simplifies incremental ingestion and schema evolution.
