from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BootcampDay1").getOrCreate()

print(spark.version)