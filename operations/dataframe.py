from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession
spark = SparkSession.builder \
    .appName("DataFrame Examples") \
    .getOrCreate()

# Method 1: Simple list with schema inference
data = [(1, "John", 30),
        (2, "Alice", 25),
        (3, "Bob", 35)]
df1 = spark.createDataFrame(data, ["id", "name", "age"])

# Method 2: Using explicit schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True)
])
df2 = spark.createDataFrame(data, schema)

"""

"""