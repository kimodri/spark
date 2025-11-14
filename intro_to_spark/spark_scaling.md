# Leveraging Scale
- Using pyspark, speed and efficient processing is the goal
- Understanding pyspark execution gets even more efficiencies
- use broadcast to manage the whole cluster

```python
# broadcasting a sample efficiency
joined_df = larged_df.join(
    broadcast(small_df), on="key_column", how="inner"
)

joined_df.show()
```
---
## Execution Plans
whenever we run an operation on a dataframe, spark constructs an execution plan which then optimizes to a physical plan

- Physical plan outlines how spark will execute the task on a distributed cluster

```python
df.filter(df.Age > 40).select("Name").explian().explain()
```
```shell
== Physical Plan ==
*(1) Filter (isnotnull(Age) AND (Age > 30))
+- Scan ExistingRDD[Name:String, Age:Int]
```
---
## Caching and Persisting DataFrames for Optimization

Caching and persisting are fundamental optimization techniques in Spark. They are involved in optimization because they directly address Spark's **lazy evaluation model** and reduce expensive **Input/Output (I/O)** and **recomputation**.

### The Optimization: Avoiding Recomputation

Spark's **lazy evaluation** means transformations are not executed until an action is called. If you run multiple actions on a DataFrame, Spark must **re-execute the entire lineage of transformations** from the source every time. Caching interrupts this process.

1.  **Without Caching:** Spark recomputes everything from the source data (e.g., reading a massive file, applying complex joins) for every subsequent action.
2.  **With Caching:** The result of the DataFrame's computation is stored after the **first action**. Subsequent actions read the result directly from the cache (memory or disk) rather than re-running the long sequence of transformations. This saves significant time and cluster resources.

### 1\. Caching (`.cache()`)

The `.cache()` method is a shorthand that stores the data in **memory** first and spills to **disk** if needed (`MEMORY_AND_DISK`).

#### Sample Code
- call `.cache` before Action
```python
# 1. Long lineage of transformations (e.g., loading, joining, filtering)
df_processed = (spark.read.parquet("big_data_source")
                     .filter("status = 'ACTIVE'")
                     .withColumn("cost", F.col("price") * 0.9))

# 2. Cache the result BEFORE the first action
df_processed.cache()

# 3. First action: Triggers computation and stores the result in cache
df_processed.count() 

# 4. Subsequent actions: Read directly from cache (fast!)
df_processed.show(10)
df_processed.write.mode("overwrite").parquet("output_path") 
```

**CAVEATS**
- Very large datasets may not fir memory and thus disk (depending on the disk configuration of the cluster this may not yield a good improvement)
- If you are reading from a local network resource and have slow local disk I/O, it may be better to avoid caching

**TIPS**
- Try caching DataFrame at various points and etermine if your performace improves (how to do this)
- Cache in memory and fast SSD (this is persisting)
- If normal caching does not work we can use parquet
- Stop caching when finished

#### 1\. Try Caching DataFrame at Various Points to Determine if Performance Improves

This tip is about finding the optimal place in your complex execution plan to interrupt Spark's lazy evaluation.

  * **How to do this:** You experiment by placing the `.cache()` call on different intermediate DataFrames within your pipeline.

      * **Scenario 1: Cache the final DataFrame.** If the final DataFrame is used multiple times (e.g., you call `.count()`, then `.show()`, then `.write()`), caching the final result prevents the entire computation from running three times.
      * **Scenario 2: Cache an expensive intermediate DataFrame.** If you have a pipeline like: **Load $\rightarrow$ Complex Join $\rightarrow$ Filter $\rightarrow$ Aggregate**, and the **Complex Join** step takes 90% of the time, you should cache the DataFrame *after* the join but *before* the subsequent filter and aggregate if those steps are run repeatedly.

  * **Code Example (Testing Cache Placement):**

    ```python
    # 1. Pipeline Segment 1 (e.g., Slow Join)
    df_join_result = df_a.join(df_b, on='key', how='inner')

    # --- START CACHE TEST HERE ---
    df_join_result.cache()
    df_join_result.count() # Action to trigger the cache
    # -----------------------------

    # 2. Pipeline Segment 2 (Subsequent Filters)
    df_final = df_join_result.filter(F.col("valid") == True)

    # 3. Time the execution of a final action here.
    # If the subsequent steps (2 and 3) are run many times, caching at step 1 saves time.
    df_final.show()
    ```
---
### 2\. Persisting (`.persist()`)

The `.persist()` method allows you to select a specific **StorageLevel** to fine-tune resource usage and fault tolerance.

#### Sample Code

To use a specific persistence level, you need to import the `StorageLevel` object:

```python
from pyspark.storage import StorageLevel

# Store serialized data to use less memory
df_ml_features.persist(StorageLevel.MEMORY_ONLY_SER) 

# Run a long ML process that uses the DataFrame many times
for i in range(10):
    model = train_model(df_ml_features, iterations=i)

# Free up memory after the job is done
df_ml_features.unpersist()
```

-----

## Storage Levels and Optimization Trade-Offs

The choice of storage level is a trade-off between **speed**, **memory usage**, and **fault tolerance**.

| Storage Level | Primary Location | Optimization Benefit | Trade-Off |
| :--- | :--- | :--- | :--- |
| **`MEMORY_ONLY`** | **Memory (RAM)** | **Fastest Access:** Avoids both I/O and CPU recomputation. | **High Memory Use:** If data doesn't fit, it must be recomputed upon access. |
| **`MEMORY_ONLY_SER`**| **Memory (RAM), Serialized**| **Memory Efficiency:** Uses less memory (by compacting data) than `MEMORY_ONLY`. | **Slower Access:** Requires extra CPU time to deserialize (uncompress) the data upon read. |
| **`MEMORY_AND_DISK`**| **Memory, then Disk** | **Fault Tolerance:** Guarantees no recomputation for lost partitions (spills to disk instead). | **Slower I/O:** Reading from disk is much slower than reading from memory. |
| **`DISK_ONLY`** | **Disk (Local)** | **Saves RAM:** Frees up memory for other tasks. | **Slowest Access:** Heaviest reliance on Disk I/O. |

## Best Practices
- Small subsections: the more data that gets used, the slower the operation: Pick tools like `map()` over `groupby()` due to selectivity of methods
- Broadcast joins: Broadcast will use all compute, even on smaller datasets
- Avoid repeated actions: repeated actions on the same data costs time and compute, without any benefit
This section discusses two key optimization strategies in distributed computing: choosing efficient low-level transformations, and leveraging broadcast mechanisms for joins.

## 1\. Small Subsections: Map vs. GroupBy for Selectivity

The advice "Pick tools like `map()` over `groupby()` due to selectivity of methods" refers to minimizing the use of **wide transformations** (operations that require shuffling data across the network) in RDDs.

### Optimization Principle

  * **Wide Transformations (Slow):** Operations like **`groupByKey()`**, **`reduceByKey()`**, **`join()`** (without broadcast), and **`distinct()`** require Spark to move data across the network to group common elements together. This process, called **shuffling**, is the single biggest performance bottleneck in Spark due to network and disk I/O.
  * **Narrow Transformations (Fast):** Operations like **`map()`**, **`filter()`**, and **`union()`** operate on a single partition of data independently and do not require shuffling.

### The Recommendation

The recommendation suggests that if you can achieve your goal using a **narrow transformation** like `map()` instead of a **wide transformation** like `groupByKey()` (the RDD equivalent of DataFrame's `groupBy()`), you should do so to avoid a costly shuffle.

### Code Example (Word Count)

A classic example is counting word frequencies. Using `groupByKey()` is less efficient than using `reduceByKey()`.

#### **Inefficient (Using GroupByKey - Wide Transformation)**

This requires shuffling all data to group keys, and then performing the aggregation on the grouped data.

```python
# Create a Pair RDD (word, 1)
rdd_pair = sc.parallelize(["apple", "banana", "apple"]).map(lambda x: (x, 1))

# **SLOW:** GroupByKey shuffles ALL (key, 1) pairs across the network
# (apple, [1, 1]), (banana, [1])
rdd_grouped = rdd_pair.groupByKey()

# Then sum the values locally
word_counts = rdd_grouped.mapValues(sum)
```

#### **Efficient (Using ReduceByKey - Wide Transformation)**

`reduceByKey()` performs a **local aggregation** (combines the counts for the same key within a partition) *before* the shuffle. This dramatically reduces the amount of data sent across the network.

```python
# Create a Pair RDD (word, 1)
rdd_pair = sc.parallelize(["apple", "banana", "apple"]).map(lambda x: (x, 1))

# **FAST:** ReduceByKey performs a local sum before shuffling
word_counts = rdd_pair.reduceByKey(lambda a, b: a + b)
```

| Method | Shuffling | Memory Use | Performance |
| :--- | :--- | :--- | :--- |
| **`groupByKey()`** | High (sends all values) | High | Poor |
| **`reduceByKey()`** | Low (sends aggregated values) | Low | Good |

-----

## 2\. Broadcast Joins

The advice "Broadcast will use all compute, even on smaller datasets" refers to the specific behavior of a **broadcast hash join**, which is an extremely fast type of join when one DataFrame is small.

### Optimization Principle

A standard hash join requires **shuffling** both DataFrames to ensure that rows with matching keys end up on the same worker node. If one DataFrame is **small** (typically under 10MB to 100MB, depending on configuration), this shuffling can be avoided.

A **broadcast join** works as follows:

1.  The small DataFrame is **collected** onto the driver.
2.  The driver **sends** (broadcasts) the entire small DataFrame to **every worker node's memory** as a local lookup table.
3.  The large DataFrame is then joined against this local copy **without any shuffling**, making the join extremely fast.

### Clarifying the Statement

The statement "Broadcast will use all compute, even on smaller datasets" is a slightly confusing way of saying:

1.  **"Use all compute"** refers to the fact that the small table is copied into the memory of *every single worker* in your cluster, consuming memory across the entire cluster.
2.  **"Even on smaller datasets"** means that while the small table itself might fit in the driver's memory, if it's too large to fit in the memory of the workers, the broadcast will fail or cause memory issues (OOM errors). The benefit is so high that you *want* to use it, but you must ensure the small table is indeed small enough.

### Code Example

In PySpark, you explicitly hint to Spark that it should use a broadcast join using the `broadcast` function.

```python
from pyspark.sql.functions import broadcast

# df_large is a massive transaction table
# df_small is a small lookup table (e.g., Department names)

# 1. Spark will broadcast df_small to all worker nodes.
# 2. The join will be executed locally on each worker without network shuffle.
df_joined = df_large.join(
    broadcast(df_small),  # Explicitly hint to broadcast df_small
    on=df_large["dept_id"] == df_small["id"],
    how="left"
)
```

| Feature | Standard Join | Broadcast Join |
| :--- | :--- | :--- |
| **Data Movement** | Shuffles both large tables. | Shuffles **only** the small table once to the workers. |
| **Speed** | Varies (often slow). | **Extremely Fast** (avoids network I/O). |
| **Memory Risk** | Low risk of OOM on driver. | **High risk** if the broadcast table is too large for worker memory. |