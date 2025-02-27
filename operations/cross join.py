from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CrossJoinExample").getOrCreate()

# Create sample DataFrames
df1 = spark.createDataFrame([
    (1, "A"),
    (2, "B")
], ["id1", "value1"])

df2 = spark.createDataFrame([
    (3, "X"),
    (4, "Y"),
    (5, "Z")
], ["id2", "value2"])

# Perform cross join
result = df1.crossJoin(df2)

# Show the result
result.show()

"""
+---+------+---+------+
|id1|value1|id2|value2|
+---+------+---+------+
|  1|     A|  3|     X|
|  1|     A|  4|     Y|
|  1|     A|  5|     Z|
|  2|     B|  3|     X|
|  2|     B|  4|     Y|
|  2|     B|  5|     Z|
+---+------+---+------+

"""