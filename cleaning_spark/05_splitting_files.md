## Improving Import Performance by Splitting Files

When importing large datasets into Spark, file structure can significantly affect performance. Spark can process data in parallel, so how data is divided across files directly influences the speed of import operations.

### 1. Background

Assume you have a dataset representing departure records. You can store this dataset in two ways:

* As a **single large file**:

  ```
  departures_full.txt.gz
  ```
* Or as **multiple smaller files**, each containing a subset of rows:

  ```
  departures_000.txt.gz
  departures_001.txt.gz
  ...
  departures_013.txt.gz
  ```

Both formats contain the same total number of rows, but the data is distributed differently across files.

### 2. Code Example

```python
import time

# Import the full and split files into DataFrames
full_df = spark.read.csv('departures_full.txt.gz')
split_df = spark.read.csv('departures_*.txt.gz')

# Print the count and run time for each DataFrame
start_time_a = time.time()
print("Total rows in full DataFrame:\t%d" % full_df.count())
print("Time to run: %f" % (time.time() - start_time_a))

start_time_b = time.time()
print("Total rows in split DataFrame:\t%d" % split_df.count())
print("Time to run: %f" % (time.time() - start_time_b))
```

### 3. What the Asterisk (`*`) Means

The expression:

```python
split_df = spark.read.csv('departures_*.txt.gz')
```

instructs Spark to read **all files that match the pattern** `departures_*.txt.gz`.

In this case, it loads all files whose names start with `departures_` and end with `.txt.gz`.
That includes:

```
departures_000.txt.gz
departures_001.txt.gz
...
departures_013.txt.gz
```

Spark automatically combines these files into **one DataFrame**, processing them in parallel.

### 4. Why File Splitting Matters

Spark reads data in **parallel tasks** distributed across worker nodes. Each file or block of data can be handled by a separate task.
The impact of splitting depends on file and cluster characteristics:

| Scenario                   | Description                                                                                 | Potential Result                                  |
| -------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| **Single large file**      | Spark reads the file in chunks, but a single compressed file (`.gz`) may not be splittable. | Limited parallelism, slower import.               |
| **Multiple smaller files** | Spark can assign each file to different tasks for concurrent reading.                       | Better parallelism and faster import.             |
| **Too many small files**   | If each file is too small, task scheduling overhead becomes high.                           | Possible slowdown due to excessive file handling. |

### 5. Guidelines

* **For compressed files (`.gz`)**, splitting into multiple smaller files often improves performance because gzip compression prevents Spark from splitting a single file internally.
* **For uncompressed or splittable formats** (e.g., Parquet, ORC), splitting is less critical because Spark can process a single file in parallel.
* **Avoid thousands of very small files**; instead, aim for file sizes that align with your cluster’s block size (typically 128 MB to 256 MB).

### 6. Summary

| File Structure              | Parallelism                 | Import Speed | Notes                       |
| --------------------------- | --------------------------- | ------------ | --------------------------- |
| Single large `.gz` file     | Low                         | Slow         | Gzip is not splittable      |
| Multiple medium `.gz` files | High                        | Fast         | Recommended                 |
| Many tiny files             | Very high (but inefficient) | Slow         | Avoid excessive small files |

By adjusting file structure to match Spark’s parallel processing model, you can achieve faster data imports and better resource utilization.

