from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Row Examples") \
    .getOrCreate()

# Create Row objects
row1 = Row(id=1, name="John", age=30)
row2 = Row(id=2, name="Alice", age=25)
row3 = Row(id=3, name="Bob", age=35)

# Create DataFrame from Rows
df = spark.createDataFrame([row1, row2, row3])

# Select specific rows
df.select("name", "age").show()

# Filter rows
df.filter(df.age > 25).show()

# Get first N rows
first_two = df.limit(2).collect()

# Get distinct rows
df.distinct().show()

# Sort rows
df.orderBy("age").show()
df.orderBy(desc("age")).show()

"""
+-----+---+
| name|age|
+-----+---+
| John| 30|
|Alice| 25|
|  Bob| 35|
+-----+---+

+---+----+---+
| id|name|age|
+---+----+---+
|  1|John| 30|
|  3| Bob| 35|
+---+----+---+

+---+-----+---+
| id| name|age|
+---+-----+---+
|  1| John| 30|
|  3|  Bob| 35|
|  2|Alice| 25|
+---+-----+---+

+---+-----+---+
| id| name|age|
+---+-----+---+
|  2|Alice| 25|
|  1| John| 30|
|  3|  Bob| 35|
+---+-----+---+

+---+-----+---+
| id| name|age|
+---+-----+---+
|  3|  Bob| 35|
|  1| John| 30|
|  2|Alice| 25|
+---+-----+---+

"""