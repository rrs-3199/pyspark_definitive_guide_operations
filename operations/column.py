from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("Column Examples").getOrCreate()

# Sample DataFrame
data = [
    (1, "John", 30, 50000),
    (2, "Alice", 25, 60000),
    (3, "Bob", 35, 45000)
]
df = spark.createDataFrame(data, ["id", "name", "age", "salary"])

# Different ways to reference columns
df.select("name").show()  # Using column name as string
df.select(df.name).show() # Using DataFrame.column_name
df.select(col("name")).show()  # Using col() function
df.select(column("name")).show()  # Using column() function

"""
+-----+
| name|
+-----+
| John|
|Alice|
|  Bob|
+-----+

+-----+
| name|
+-----+
| John|
|Alice|
|  Bob|
+-----+

+-----+
| name|
+-----+
| John|
|Alice|
|  Bob|
+-----+

+-----+
| name|
+-----+
| John|
|Alice|
|  Bob|
+-----+
"""