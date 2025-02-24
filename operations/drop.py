from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DropColumnExample") \
    .getOrCreate()

# Create a sample DataFrame
data = [
    (1, "John", 30, "New York", "Engineering"),
    (2, "Alice", 25, "San Francisco", "Marketing"),
    (3, "Bob", 35, "Chicago", "Sales"),
    (4, "Emma", 28, "Boston", "Engineering")
]
columns = ["id", "name", "age", "city", "department"]
df = spark.createDataFrame(data, columns)

# Method 1: Drop a single column
df_drop_single = df.drop("age")

# Method 2: Drop multiple columns
df_drop_multiple = df.drop("age", "city")

# Method 3: Drop columns using a list
columns_to_drop = ["age", "department"]
df_drop_list = df.drop(*columns_to_drop)

# Method 4: Drop columns conditionally
# Example: Drop columns that contain null values in more than 50% of rows
threshold = 0.5
for col_name in df.columns:
    null_fraction = df.filter(col(col_name).isNull()).count() / df.count()
    if null_fraction > threshold:
        df = df.drop(col_name)

# Show results
print("Original DataFrame:")
df.show()

print("\nAfter dropping single column (age):")
df_drop_single.show()

print("\nAfter dropping multiple columns (age, city):")
df_drop_multiple.show()

print("\nAfter dropping columns from list (age, department):")
df_drop_list.show()

# Clean up
spark.stop()

Original DataFrame:
+---+-----+---+-------------+-----------+
| id| name|age|         city| department|
+---+-----+---+-------------+-----------+
|  1| John| 30|     New York|Engineering|
|  2|Alice| 25|San Francisco|  Marketing|
|  3|  Bob| 35|      Chicago|      Sales|
|  4| Emma| 28|       Boston|Engineering|
+---+-----+---+-------------+-----------+


After dropping single column (age):
+---+-----+-------------+-----------+
| id| name|         city| department|
+---+-----+-------------+-----------+
|  1| John|     New York|Engineering|
|  2|Alice|San Francisco|  Marketing|
|  3|  Bob|      Chicago|      Sales|
|  4| Emma|       Boston|Engineering|
+---+-----+-------------+-----------+


After dropping multiple columns (age, city):
+---+-----+-----------+
| id| name| department|
+---+-----+-----------+
|  1| John|Engineering|
|  2|Alice|  Marketing|
|  3|  Bob|      Sales|
|  4| Emma|Engineering|
+---+-----+-----------+


After dropping columns from list (age, department):
+---+-----+-------------+
| id| name|         city|
+---+-----+-------------+
|  1| John|     New York|
|  2|Alice|San Francisco|
|  3|  Bob|      Chicago|
|  4| Emma|       Boston|
+---+-----+-------------+