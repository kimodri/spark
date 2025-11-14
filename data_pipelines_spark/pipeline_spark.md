The typical pipeline follows:
- Reading the data
- Transforming it
- Validation
- Analysis
- Loading the data (output)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import monotonically_increasing_id

# Initialize Spark session
spark = SparkSession.builder.appName("CSV to Parquet and JSON").getOrCreate()

# Define schema
schema = StructType([
    StructField('name', StringType(), nullable=False),
    StructField('age', StringType(), nullable=False)
])

# Read CSV with schema
df = spark.read.format('csv').option('header', 'false').schema(schema).load('datafile')

# Add unique ID column
df = df.withColumn('id', monotonically_increasing_id())

...

# Write to Parquet
df.write.mode('overwrite').parquet('outdata.parquet')

# Write to JSON
df.write.mode('overwrite').json('outdata.json')
```

## Parsing Data
Spark's csv parser
- automatically removes blank lines
- remove comments using an optional argument

```python
df1 = spark.read.csv('data', comment='#')
# sep
# header
# schema
```
