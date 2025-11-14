## Measuring Execution Time in PySpark

You measure time by recording the start time just before the Spark **Action** and the end time right after the Action completes.

### 1\. Using Python's `time` Module

The most common way to time a section of code is using the `time.time()` function.

```python
import time
from pyspark.sql import functions as F

# --- Step 1: Define the lazy transformations ---
df_processed = df.filter(F.col("Status") == "ACTIVE") \
                 .groupBy("Department").sum("Salary")

# --- Step 2: Record Start Time ---
start_time = time.time()

# --- Step 3: Trigger a Spark Action (e.g., collect, count, write) ---
# This is where all the previous transformations are executed.
result = df_processed.count()

# --- Step 4: Record End Time ---
end_time = time.time()

# --- Step 5: Calculate Duration ---
duration = end_time - start_time

print(f"Total rows counted: {result}")
print(f"Execution Time: {duration:.2f} seconds")
```

### 2\. Why Time an Action?

It is crucial to time a Spark **Action** (like `.count()`, `.show()`, `.collect()`, or `.write()`) and not just the transformations (like `.filter()` or `.withColumn()`).

  * **Transformations are Lazy:** If you timed only the line `df_processed = df.filter(...)`, the time would be near zero because Spark has only recorded the *plan*, not actually executed the computation.
  * **Actions are Eager:** Only when an Action is called does Spark's Catalyst Optimizer compile the plan and execute the distributed workload across the cluster. The time recorded reflects the true cost of the computation.

### 3\. Using Caching to Isolate Costs

To measure the performance improvement of a cache (as we discussed previously), you must time the operation twice:

1.  **Time 1 (Initial Run):** Time the first action. This time includes the cost of **computation** and **caching**.
2.  **Time 2 (Cached Run):** Immediately time the same action again. This time should be significantly shorter, as it only measures the cost of **reading data from memory**.

<!-- end list -->

```python
# First Action (Computation + Caching)
df_processed.cache()
start1 = time.time()
df_processed.count() # Triggers computation and stores data
end1 = time.time()

# Second Action (Reading from Cache Only)
start2 = time.time()
df_processed.count() # Reads directly from cache
end2 = time.time()

print(f"Time 1 (Computation): {end1 - start1:.2f} seconds")
print(f"Time 2 (Cached Read): {end2 - start2:.2f} seconds")
```