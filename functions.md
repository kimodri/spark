
# 🧹 PySpark Data Cleaning & Preprocessing Reference

## ⚙️ 1. Null Handling

Functions to detect, replace, or remove null and missing values.

| Function / Method                     | Description                                         | Example                                                                                   |
| ------------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `df.na.fill()`                        | Fill null values with a constant or dict per column | `df.na.fill({'age': 0, 'city': 'Unknown'})`                                               |
| `df.na.drop()`                        | Drop rows with null values                          | `df.na.drop(subset=['email'])`                                                            |
| `df.na.replace()`                     | Replace specific values (including null)            | `df.na.replace('', None)`                                                                 |
| `F.coalesce(col1, col2, ...)`         | Return the first non-null value                     | `df.withColumn('final', F.coalesce('val1', 'val2'))`                                      |
| `F.when(F.col(...).isNull(), ...)`    | Conditional replacement                             | `df.withColumn('salary', F.when(F.col('salary').isNull(), 0).otherwise(F.col('salary')))` |
| `df.filter(F.col('col').isNotNull())` | Keep only non-null rows                             | `df.filter(F.col('email').isNotNull())`                                                   |

---

## ✂️ 2. String Cleaning & Standardization

| Function / Method                             | Description                                 | Example                                                           |
| --------------------------------------------- | ------------------------------------------- | ----------------------------------------------------------------- |
| `F.trim(col)`                                 | Remove leading and trailing spaces          | `df.withColumn('name', F.trim('name'))`                           |
| `F.ltrim(col)` / `F.rtrim(col)`               | Left/right trim only                        | `df.withColumn('id', F.ltrim('id'))`                              |
| `F.lower(col)` / `F.upper(col)`               | Convert case                                | `df.withColumn('email', F.lower('email'))`                        |
| `F.regexp_replace(col, pattern, replacement)` | Regex-based replacement                     | `df.withColumn('phone', F.regexp_replace('phone', '[^0-9]', ''))` |
| `F.translate(col, "abc", "xyz")`              | Replace characters one-to-one               | `F.translate('status', 'YN', '10')`                               |
| `F.substring(col, start, length)`             | Extract substring                           | `df.withColumn('prefix', F.substring('code', 1, 3))`              |
| `F.concat_ws(delimiter, *cols)`               | Concatenate multiple columns with delimiter | `F.concat_ws(' ', 'fname', 'lname')`                              |

**Tip:** Combine `trim` + `lower` + `regexp_replace` to standardize messy categorical data (like “ Yes ”, “YES”, “yes ” → “yes”).

---

## 🔢 3. Numeric Data Cleaning

| Function / Method                | Description                                | Example                                                  |
| -------------------------------- | ------------------------------------------ | -------------------------------------------------------- |
| `F.round(col, n)`                | Round to *n* decimal places                | `df.withColumn('price', F.round('price', 2))`            |
| `F.abs(col)`                     | Absolute value                             | `df.withColumn('error', F.abs('error'))`                 |
| `F.when(...).otherwise(...)`     | Conditional replacement for invalid values | `F.when(F.col('age') < 0, None).otherwise(F.col('age'))` |
| `df.filter(F.col('amount') > 0)` | Filter out invalid or negative data        | `df.filter(F.col('amount') >= 0)`                        |
| `F.cast('col', 'type')`          | Convert type safely                        | `df.withColumn('age', F.col('age').cast('int'))`         |

**Tip:** Always cast numeric columns explicitly when reading from CSV/JSON; Spark may infer as string.

---

## 📅 4. Date and Time Handling

| Function / Method                                    | Description                 | Example                                           |
| ---------------------------------------------------- | --------------------------- | ------------------------------------------------- |
| `F.to_date(col, format)`                             | Convert string to date      | `F.to_date('date_str', 'yyyy-MM-dd')`             |
| `F.to_timestamp(col, format)`                        | Convert string to timestamp | `F.to_timestamp('ts_str', 'yyyy-MM-dd HH:mm:ss')` |
| `F.date_format(col, format)`                         | Format date into string     | `F.date_format('date_col', 'MM/dd/yyyy')`         |
| `F.year(col)` / `F.month(col)` / `F.dayofmonth(col)` | Extract components          | `F.year('date_col')`                              |
| `F.datediff(end, start)`                             | Difference in days          | `F.datediff('end_date', 'start_date')`            |
| `F.add_months(col, n)`                               | Add/subtract months         | `F.add_months('date_col', 1)`                     |
| `F.current_date()` / `F.current_timestamp()`         | Current date/time           | `F.current_timestamp()`                           |

**Tip:** Use `to_date()` to ensure date columns are comparable and sortable.

---

## 🧾 5. Deduplication and Uniqueness

| Function / Method | Description | Example |
| :--- | :--- | :--- |
| `df.dropDuplicates()` | Removes fully identical rows across all columns. | `df = df.dropDuplicates()` |
| `df.dropDuplicates(subset=['col1', 'col2'])` | Removes duplicates based on specific column(s). | `df = df.dropDuplicates(['email'])` |
| `F.row_number().over(window)` | Retain only the first or most recent record within a group. | ```python<br>from pyspark.sql.window import Window<br>w = Window.partitionBy('user_id').orderBy(F.desc('timestamp'))<br>df = df.withColumn('rn', F.row_number().over(w)).filter('rn = 1')``` |
| `F.countDistinct(col)` | Count distinct values (for validation). | `df.select(F.countDistinct('user_id'))` |

---

**Tip:** Combine `dropDuplicates` with sorting logic using `Window` to ensure only the latest record is retained.

## 🧪 6. Data Validation & Filtering Invalid Data

| Function / Method | Description | Example |
|--------------------|--------------|----------|
| `df.filter(condition)` | Filter rows that meet a condition. | `df.filter(F.col('age') > 0)` |
| `F.col('column').rlike('regex')` | Regex-based validation. | `df.filter(F.col('email').rlike('^[\\w.%+-]+@[\\w.-]+\\.[A-Za-z]{2,}$'))` |
| `F.length(col)` | Validate string length. | `df.filter(F.length('phone') == 11)` |
| `F.isnan(col)` / `F.isnull(col)` | Identify invalid numeric or null entries. | `df.filter(~F.isnan('value'))` |
| `F.when(...).otherwise(...)` | Replace invalid values with defaults. | `df.withColumn('score', F.when(F.col('score') < 0, 0).otherwise(F.col('score')))` |
| `df.dropna(subset=[...])` | Drop rows missing key fields. | `df.dropna(subset=['id', 'email'])` |

**Tip:** For data validation, it’s common to define “valid ranges” or “valid patterns” and then filter rows that don’t comply.

---


## 📏 7. Normalization and Scaling

| Function / Method | Description | Example |
| :--- | :--- | :--- |
| `F.round(col, n)` | Round numeric values to $n$ decimal places. | `df.withColumn('price', F.round('price', 2))` |
| `F.abs(col)` | Calculate the **absolute value** of a numeric column. | `df.withColumn('error', F.abs('error'))` |
| `F.log(col)` / `F.log1p(col)` | Perform a **Log-transform** (useful for highly skewed data). | `df.withColumn('log_income', F.log1p('income'))` |
| `F.col('col') / F.sum('col').over(Window())` | **Scale a value relative to the total** (e.g., calculating a percentage share). | ```python<br>w = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)<br>df.withColumn('share', F.col('sales') / F.sum('sales').over(w))<br>``` |
| `F.mean()` and `F.stddev()` | Functions used for manual **Z-score normalization** (Standard Scaling). | `F.col('val') - F.mean('val') / F.stddev('val')` |


> **Tip:** For machine learning feature engineering, Spark **MLlib** provides specialized transformers like `StandardScaler` (for Z-score normalization) and `MinMaxScaler` (for Min-Max scaling) which are generally preferred over manual column operations.

---

## 🧮 8. Type Casting and Conversion

| Function / Method | Description | Example |
|--------------------|--------------|----------|
| `F.col('col').cast('type')` | Convert column type (string → int, etc.) | `df.withColumn('age', F.col('age').cast('int'))` |
| `F.to_date(col, format)` | Convert string to date. | `F.to_date('dob', 'yyyy-MM-dd')` |
| `F.to_timestamp(col, format)` | Convert string to timestamp. | `F.to_timestamp('created_at', 'yyyy-MM-dd HH:mm:ss')` |
| `F.unix_timestamp(col, format)` | Get Unix time from date string. | `F.unix_timestamp('created_at', 'yyyy-MM-dd HH:mm:ss')` |
| `df.selectExpr('cast(column as int)')` | SQL-style casting. | `df.selectExpr('cast(salary as double)')` |

**Tip:** Always cast explicitly when reading from CSV/JSON—Spark may infer all columns as strings.

---

## 🔡 9. Column and Schema Cleaning

| Function / Method | Description | Example |
|--------------------|--------------|----------|
| `df.withColumnRenamed(old, new)` | Rename a single column. | `df.withColumnRenamed('empName', 'employee_name')` |
| `df.toDF(*new_names)` | Rename multiple columns. | `df.toDF('id', 'name', 'dept')` |
| `df.drop('col1', 'col2')` | Drop unnecessary columns. | `df.drop('temp_col')` |
| `df.select([F.col(c).alias(c.lower()) for c in df.columns])` | Normalize all column names. | Converts all column names to lowercase. |

**Tip:** For large pipelines, keep consistent naming conventions (e.g., snake_case, no spaces).

---

## 🧠 10. Feature Preparation / Derived Columns

| Function / Method | Description | Example |
|--------------------|--------------|----------|
| `F.concat_ws(delimiter, *cols)` | Combine multiple columns. | `F.concat_ws('-', 'country', 'city')` |
| `F.lit(value)` | Add a constant column. | `df.withColumn('source', F.lit('raw_import'))` |
| `F.split(col, delimiter)` | Split strings into arrays. | `F.split('tags', ',')` |
| `F.size(array_col)` | Get array length. | `F.size('skills')` |
| `F.explode(array_col)` | Flatten arrays into multiple rows. | `df.withColumn('skill', F.explode('skills'))` |
| `F.struct(*cols)` | Combine columns into a struct. | `F.struct('lat', 'lon').alias('geo')` |
| `F.colRegex("`regex`")` | Select multiple columns by pattern. | `df.select(F.colRegex("`^sales_.*`"))` |

**Tip:** `explode` and `struct` are particularly useful when normalizing nested JSON or array data into tabular form.

---

## 🧰 11. Miscellaneous but Practical Data Cleaning Utilities

| Function / Method | Description | Example |
|--------------------|--------------|----------|
| `F.monotonically_increasing_id()` | Generate unique IDs for rows. | `df.withColumn('uid', F.monotonically_increasing_id())` |
| `F.hash(*cols)` | Create a deterministic hash (for keys). | `df.withColumn('hash', F.hash('col1', 'col2'))` |
| `df.sample(fraction, seed)` | Randomly sample data. | `df.sample(0.1, seed=42)` |
| `df.limit(n)` | Limit number of rows. | `df.limit(1000)` |
| `F.broadcast(df)` | Optimize joins with small lookup tables. | `df.join(F.broadcast(dim_table), 'id')` |

---

## 🧾 Summary: Cleaning Pipeline Blueprint

Typical **data cleaning workflow** in PySpark:

```python
from pyspark.sql import functions as F, Window

df_clean = (
    df
    # 1️⃣ Handle nulls
    .na.fill({'age': 0, 'city': 'Unknown'})
    # 2️⃣ Standardize text
    .withColumn('email', F.lower(F.trim('email')))
    # 3️⃣ Remove invalid or duplicate rows
    .filter(F.col('age') >= 0)
    .dropDuplicates(['user_id'])
    # 4️⃣ Type casting and formatting
    .withColumn('date', F.to_date('date', 'yyyy-MM-dd'))
    # 5️⃣ Feature derivation
    .withColumn('year', F.year('date'))
)
