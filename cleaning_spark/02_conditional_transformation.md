# Condtional Transformations
## `.when(<if condition>, <then x>)`

The `.when()` function allows you to evaluate a condition and return a specific value if that condition is true. It is almost always used within the `.withColumn()` method to create or modify a column.

### Syntax and Structure

The function is used sequentially:

1.  **Start:** Begin with one `.when()` call to evaluate the first condition.
2.  **Continue:** Chain multiple `.when()` calls for `else if` logic.
3.  **End:** Conclude with an optional `.otherwise()` call for the final `else` logic if none of the preceding conditions were met.

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "NewColumn",
    F.when(
        # <if condition 1>
        F.col("Score") >= 90, 
        # <then x 1>
        F.lit("A") 
    ).when(
        # <else if condition 2>
        F.col("Score") >= 80, 
        # <then x 2>
        F.lit("B")
    ).otherwise(
        # <else>
        F.lit("F")
    )
)
```

### Key Components

  * **`F.when(condition, value)`:**
      * The **`condition`** must be a column expression that evaluates to a Boolean (e.g., `F.col("Age") > 18`).
      * The **`value`** is the value the column will take if the condition is true. This can be a literal value (using `F.lit()`) or another column's value (using `F.col()`).
  * **`F.otherwise(value)`:** This is optional, but highly recommended. It specifies the default value if none of the preceding `.when()` conditions are met. If omitted, rows that don't satisfy any condition will be assigned `null`.

### Scenario: Categorizing Sales Performance

You want to create a new column, **`SalesCategory`**, based on the value in the existing **`SalesAmount`** column.

| Condition | `SalesCategory` Value |
| :--- | :--- |
| If `SalesAmount` is over 1000 | **"High Performer"** |
| If `SalesAmount` is between 500 and 1000 (inclusive) | **"Average Performer"** |
| Otherwise | **"Low Performer"** |

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "SalesCategory",
    F.when(
        F.col("SalesAmount") > 1000, 
        F.lit("High Performer")
    ).when(
        F.col("SalesAmount") >= 500,  # Note: 500 to 1000, as >1000 was handled above
        F.lit("Average Performer")
    ).otherwise(
        F.lit("Low Performer")
    )
)
```

In this example, the conditions are evaluated in order. A sale of 1500 hits the first `.when()` and is assigned "High Performer." A sale of 700 skips the first, hits the second, and is assigned "Average Performer." A sale of 300 skips both and is assigned "Low Performer" by the `.otherwise()`.