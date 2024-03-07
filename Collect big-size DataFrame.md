1. How to avoid using collect() operation in pyspark?
1) To call toPandas with Arrow enabled. But we still should avoid collect even with toPandas. Because with toPandas, dataframe is stored in columnar format instead of row-based format.
e.g.,
# Enable Arrow optimization
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Create a Spark DataFrame
df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
# Convert to a Pandas DataFrame using Arrow
pdf = df.toPandas()

2) To write the big-size DataFrame to files and then read them.
df.write.format(“parquet”).save(“path”)
spark.read.parquet(“path”) to read it back
