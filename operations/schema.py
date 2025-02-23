from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Schema Examples") \
    .getOrCreate()

# 1. Basic Schema
basic_schema = StructType([
    StructField("id", IntegerType(), False),     # Not nullable
    StructField("name", StringType(), True),     # Nullable
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [
    (1, "John", 30, "New York"),
    (2, "Alice", 25, "San Francisco")
]

df = spark.createDataFrame(data, basic_schema)
df.show()
df.printSchema()

# 2. Schema with Different Data Types
detailed_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("is_manager", BooleanType(), True),
    StructField("join_date", DateType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("rating", FloatType(), True),
    StructField("employee_code", LongType(), True)
])

# 3. Schema with Array Type
array_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("scores", ArrayType(IntegerType()), True),
    StructField("languages", ArrayType(StringType()), True)
])

array_data = [
    (1, "John", [80, 85, 90], ["Python", "Java"]),
    (2, "Alice", [85, 90, 95], ["Scala", "R"])
]

df_array = spark.createDataFrame(array_data, array_schema)

# 4. Schema with Nested Structure
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True)
])

employee_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("address", address_schema, True)
])

nested_data = [
    (1, "John", ("123 Main St", "New York", "10001")),
    (2, "Alice", ("456 Market St", "San Francisco", "94105"))
]

df_nested = spark.createDataFrame(nested_data, employee_schema)

# 5. Schema with Map Type
map_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("properties", MapType(StringType(), StringType()), True)
])

map_data = [
    (1, "John", {"dept": "IT", "location": "NY"}),
    (2, "Alice", {"dept": "HR", "location": "SF"})
]

df_map = spark.createDataFrame(map_data, map_schema)

# 6. Schema with Decimal Type (for precise calculations)
decimal_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("amount", DecimalType(10, 2), True)  # 10 digits total, 2 decimal places
])

# Print all schemas
print("Basic Schema:")
df.printSchema()

print("\nArray Schema:")
df_array.printSchema()

print("\nNested Schema:")
df_nested.printSchema()

print("\nMap Schema:")
df_map.printSchema()


"""
+---+-----+---+-------------+
| id| name|age|         city|
+---+-----+---+-------------+
|  1| John| 30|     New York|
|  2|Alice| 25|San Francisco|
+---+-----+---+-------------+

root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)

Basic Schema:
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)


Array Schema:
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- scores: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- languages: array (nullable = true)
 |    |-- element: string (containsNull = true)


Nested Schema:
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- address: struct (nullable = true)
 |    |-- street: string (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- zipcode: string (nullable = true)


Map Schema:
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- properties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
"""