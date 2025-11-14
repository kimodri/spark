Here’s a structured guide explaining **how Spark writes partitions to storage**, with diagrams to make the concept clear.

---

# Spark DataFrame Storage and Partitions

When working with Spark, understanding how **partitions relate to physical storage** is critical for optimizing performance and managing output files.

---

## 1. What Happens When You Write a DataFrame

In Spark:

* A **DataFrame is divided into partitions** in memory for parallel processing.
* **Each partition becomes a separate file** when the DataFrame is written to storage (CSV, Parquet, ORC, etc.).
* Reading the folder back into Spark **recombines all files into a single DataFrame**.

### Example

```python
# Suppose the DataFrame has 4 partitions
df.rdd.getNumPartitions()  # returns 4

# Write to storage
df.write.parquet("output/avg_delay/")
```

**Resulting files in storage:**

```
output/avg_delay/
    part-00000.parquet
    part-00001.parquet
    part-00002.parquet
    part-00003.parquet
```

**Note:** The number of files written corresponds to the number of partitions at the time of writing.

---

## 2. Controlling the Number of Output Files

### 2.1 Using `.repartition()`

Use `.repartition(n)` to **increase or evenly distribute partitions**:

```python
# Increase partitions for parallel processing
df_repartitioned = df.repartition(16)
```

* Advantage: More tasks can run in parallel for heavy computations.
* Disadvantage: Produces more files when writing to storage.

---

### 2.2 Using `.coalesce()`

Use `.coalesce(n)` to **reduce the number of partitions efficiently** without a full shuffle:

```python
# Reduce partitions before writing to storage
df_coalesced = df_repartitioned.coalesce(4)
df_coalesced.write.parquet("output/avg_delay/")
```

* Advantage: Fewer output files → easier management and better read performance.
* Important: `.coalesce()` only reduces partitions; it cannot increase them.

---

## 3. Diagram: Partitions → Files → DataFrame

```
Step 1: DataFrame in Memory
+-----------------------+
| Partition 1           |
|-----------------------|
| Partition 2           |
|-----------------------|
| Partition 3           |
|-----------------------|
| Partition 4           |
+-----------------------+

Step 2: Writing to Storage
Memory partitions → Files
Partition 1 → part-00000.parquet
Partition 2 → part-00001.parquet
Partition 3 → part-00002.parquet
Partition 4 → part-00003.parquet

Step 3: Reading Back
Files in storage → DataFrame
part-00000.parquet +
part-00001.parquet +
part-00002.parquet +
part-00003.parquet
            ↓
         Recombined
+-----------------------+
| Partition 1           |
| Partition 2           |
| Partition 3           |
| Partition 4           |
+-----------------------+
```

* The DataFrame loaded back is **logically identical** to the original.
* Partitioning may change depending on cluster configuration and default settings.

---

## 4. Practical Recommendations

1. **Manage partition size**: Aim for partitions that are 128–256 MB in size for efficient processing.
2. **Reduce small files**: Use `.coalesce()` before writing to avoid too many tiny output files.
3. **Increase partitions for heavy computations**: Use `.repartition()` if you need more parallelism.
4. **Monitor output files**: Check the number of files written using your storage system or `dbutils.fs.ls()` (Databricks) to ensure it aligns with your expectations.

