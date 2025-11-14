# Introduction
## Understanding the Parquet
- csv is inefficient
- spark faces problems with csv (slow to parse)
- Parquet supports predicate pushdown adn stores schema information

```python
# reading the parquet
df = spark.read.format('parquet').load('path')
df = spark.read.parquet('path')

# writing parquet
df.write.format('paruqet').save('path')
df.write.parquet('path')
```

## Common DataFrame Transformation
- filter
    - remove nulls
    - remove odd entries
    - split data from combined sources
    ```python
    df.filter(df['name'].isNotNull())
    df.filter(df.date.year > 1800)
    df.filter(df['_c0'].contains('VOTE'))
    df.filter.where(~ df._c1.isNull())
    ```
- select
- withColumn
- drop: removes a column in a dataframe

## Column String Transformation
- Contained in `pyspark.sql.functions`
- Applied per column as transformation
```python
import pyspark.sql.functions as F
df.withColumn('upper', F.upper('name'))
```
- Can create intermediary columns
```python
df.withColumn('splits', F.split('name', ' '))
```
- Casting string
```python
df.withColumn('year', df['_c4'].cast(IntegerType()))
```

## ArrayType Columns
- You often face this when cleaning
- `size('col')`
- `getItem(index)`

## How and Why Intermediary Columns Work

You create intermediary columns using the `.withColumn()` method.

| Aspect | Description |
| :--- | :--- |
| **How:** | You use `.withColumn()` multiple times in a sequence, with each step relying on the output of the previous one. |
| **Why (Maintainability):** | Complex string transformations (like cleaning, splitting, and parsing dates) are easier to **debug** when broken down. If the final output is wrong, you can inspect each intermediary column to see where the process failed. |
| **Why (Reusability):** | An intermediary result might be needed for **multiple final columns**. For example, a "cleaned name" column might be used to derive both a "first initial" column and a "full salutation" column. |
| **Why (Optimization):**| By creating a single intermediary column for a complex calculation, Spark only has to compute that result once, even if you reference it multiple times in subsequent steps. |

-----

## Scenario: Normalizing and Parsing User Names

Imagine you have a raw `FullName` column, and you need to derive a clean **Last Name** and a normalized **First Name**.

### 🛠 Transformation Steps with Intermediary Columns

```python
from pyspark.sql import functions as F

# Sample Data
data = [
    ("  Dr. John SMITH, JR.",),
    ("Jane Doe (Ph.D.)",),
    ("MR. PETER Jones",),
]
df = spark.createDataFrame(data, ["FullName"])

# 1. Clean up and standardize the string (Intermediary Column 1)
df_steps = df.withColumn(
    "CleanName",
    F.trim(F.regexp_replace(F.col("FullName"), r"[^a-zA-Z\s]", "")) # Remove title/suffix, keep only letters and spaces, then trim
)

# 2. Split the name (Intermediary Column 2)
df_steps = df_steps.withColumn(
    "NameParts",
    F.split(F.col("CleanName"), " ") # Split the cleaned string by space
)

# 3. Derive the Final Columns from NameParts
df_final = df_steps.withColumn(
    "LastName",
    F.element_at(F.col("NameParts"), -1) # Get the last element of the array
).withColumn(
    "FirstName",
    F.element_at(F.col("NameParts"), 1) # Get the first element of the array
)

# Show the result, including the intermediary steps for debugging
df_final.show(truncate=False)
```

### Output

| FullName | CleanName (Intermediary 1) | NameParts (Intermediary 2) | LastName (Final) | FirstName (Final) |
| :--- | :--- | :--- | :--- | :--- |
| `   Dr. John SMITH, JR. ` | `John SMITH JR` | `[John, SMITH, JR]` | `JR` | `John` |
| `Jane Doe (Ph.D.)` | `Jane Doe` | `[Jane, Doe]` | `Doe` | `Jane` |
| `MR. PETER Jones` | `PETER Jones` | `[PETER, Jones]` | `Jones` | `PETER` |

By using `CleanName` and `NameParts` as intermediary columns, we could verify that the string was correctly cleaned before attempting the final split, significantly simplifying the complex regular expression and array indexing.

## Sample of Splitting First and Last Name
