from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Complex Types Example") \
    .getOrCreate()

# 1. Arrays
# Create a DataFrame with an array column
array_data = [
    (1, ["apple", "banana", "orange"]),
    (2, ["grape", "mango"]),
    (3, ["kiwi", "pear", "peach"])
]
array_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("fruits", ArrayType(StringType()), False)
])
array_df = spark.createDataFrame(array_data, array_schema)

# Array operations
array_df.select(
    "id",
    "fruits",
    size("fruits").alias("array_length"),
    array_contains("fruits", "apple").alias("has_apple"),
    element_at("fruits", 1).alias("first_fruit"),
    explode("fruits").alias("exploded_fruits")
).show()

# 2. Maps
# Create a DataFrame with a map column
map_data = [
    (1, {"name": "John", "age": "30"}),
    (2, {"name": "Alice", "city": "New York"}),
    (3, {"name": "Bob", "role": "developer"})
]
map_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("attributes", MapType(StringType(), StringType()), False)
])
map_df = spark.createDataFrame(map_data, map_schema)

# Map operations
map_df.select(
    "id",
    "attributes",
    map_keys("attributes").alias("keys"),
    map_values("attributes").alias("values"),
    expr("attributes['name']").alias("name")
).show()

# 3. Structs
# Create a DataFrame with a struct column
struct_data = [
    (1, ("John Doe", 30, "NYC")),
    (2, ("Jane Smith", 25, "LA")),
    (3, ("Bob Johnson", 35, "CHI"))
]
struct_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("person", StructType([
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("city", StringType(), False)
    ]), False)
])
struct_df = spark.createDataFrame(struct_data, struct_schema)

# Struct operations
struct_df.select(
    "id",
    "person",
    "person.name",
    "person.age",
    col("person").getField("city").alias("person_city")
).show()

# 4. Complex Nested Structures
# Create a DataFrame with nested complex types
nested_data = [
    (
        1,
        ["python", "scala"],
        {"experience": "5 years", "level": "senior"},
        ("John", ["java", "python"], {"dept": "engineering"})
    )
]
nested_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("skills", ArrayType(StringType()), False),
    StructField("metadata", MapType(StringType(), StringType()), False),
    StructField("profile", StructType([
        StructField("name", StringType(), False),
        StructField("languages", ArrayType(StringType()), False),
        StructField("details", MapType(StringType(), StringType()), False)
    ]), False)
])
nested_df = spark.createDataFrame(nested_data, nested_schema)

# Operations on nested structures
nested_df.select(
    "id",
    "skills",
    "metadata.experience",
    "profile.name",
    "profile.languages",
    "profile.details.dept"
).show()

# 5. Type Casting and Conversion
# Convert array to string
array_df.select(
    "id",
    concat_ws(", ", "fruits").alias("fruits_string")
).show()

# Convert map to arrays
map_df.select(
    "id",
    map_entries("attributes").alias("key_value_pairs")
).show()

# 6. User-Defined Functions (UDFs) with Complex Types
# Define a UDF that processes an array
process_array_udf = udf(lambda arr: [x.upper() for x in arr], ArrayType(StringType()))

array_df.select(
    "id",
    "fruits",
    process_array_udf("fruits").alias("uppercase_fruits")
).show()

# 7. Aggregations with Complex Types
# Collect elements into an array
array_df.groupBy() \
    .agg(
        collect_list("fruits").alias("all_fruits"),
        collect_set("fruits").alias("unique_fruits")
    ).show(truncate=False)

# 8. Window Functions with Complex Types
from pyspark.sql.window import Window

# Create a window specification
window_spec = Window.orderBy("id")

array_df.select(
    "id",
    "fruits",
    collect_list("fruits").over(window_spec).alias("cumulative_fruits")
).show(truncate=False)


+---+--------------------+------------+---------+-----------+---------------+
| id|              fruits|array_length|has_apple|first_fruit|exploded_fruits|
+---+--------------------+------------+---------+-----------+---------------+
|  1|[apple, banana, o...|           3|     true|      apple|          apple|
|  1|[apple, banana, o...|           3|     true|      apple|         banana|
|  1|[apple, banana, o...|           3|     true|      apple|         orange|
|  2|      [grape, mango]|           2|    false|      grape|          grape|
|  2|      [grape, mango]|           2|    false|      grape|          mango|
|  3| [kiwi, pear, peach]|           3|    false|       kiwi|           kiwi|
|  3| [kiwi, pear, peach]|           3|    false|       kiwi|           pear|
|  3| [kiwi, pear, peach]|           3|    false|       kiwi|          peach|
+---+--------------------+------------+---------+-----------+---------------+

+---+--------------------+------------+-----------------+-----+
| id|          attributes|        keys|           values| name|
+---+--------------------+------------+-----------------+-----+
|  1|{name -> John, ag...| [name, age]|       [John, 30]| John|
|  2|{name -> Alice, c...|[name, city]|[Alice, New York]|Alice|
|  3|{name -> Bob, rol...|[name, role]| [Bob, developer]|  Bob|
+---+--------------------+------------+-----------------+-----+

+---+--------------------+-----------+---+-----------+
| id|              person|       name|age|person_city|
+---+--------------------+-----------+---+-----------+
|  1| {John Doe, 30, NYC}|   John Doe| 30|        NYC|
|  2|{Jane Smith, 25, LA}| Jane Smith| 25|         LA|
|  3|{Bob Johnson, 35,...|Bob Johnson| 35|        CHI|
+---+--------------------+-----------+---+-----------+

+---+---------------+----------+----+--------------+-----------+
| id|         skills|experience|name|     languages|       dept|
+---+---------------+----------+----+--------------+-----------+
|  1|[python, scala]|   5 years|John|[java, python]|engineering|
+---+---------------+----------+----+--------------+-----------+

+---+--------------------+
| id|       fruits_string|
+---+--------------------+
|  1|apple, banana, or...|
|  2|        grape, mango|
|  3|   kiwi, pear, peach|
+---+--------------------+

+---+--------------------+
| id|     key_value_pairs|
+---+--------------------+
|  1|[{name, John}, {a...|
|  2|[{name, Alice}, {...|
|  3|[{name, Bob}, {ro...|
+---+--------------------+

+---+--------------------+--------------------+
| id|              fruits|    uppercase_fruits|
+---+--------------------+--------------------+
|  1|[apple, banana, o...|[APPLE, BANANA, O...|
|  2|      [grape, mango]|      [GRAPE, MANGO]|
|  3| [kiwi, pear, peach]| [KIWI, PEAR, PEACH]|
+---+--------------------+--------------------+

+--------------------------------------------------------------+--------------------------------------------------------------+
|all_fruits                                                    |unique_fruits                                                 |
+--------------------------------------------------------------+--------------------------------------------------------------+
|[[apple, banana, orange], [grape, mango], [kiwi, pear, peach]]|[[apple, banana, orange], [grape, mango], [kiwi, pear, peach]]|
+--------------------------------------------------------------+--------------------------------------------------------------+

+---+-----------------------+--------------------------------------------------------------+
|id |fruits                 |cumulative_fruits                                             |
+---+-----------------------+--------------------------------------------------------------+
|1  |[apple, banana, orange]|[[apple, banana, orange]]                                     |
|2  |[grape, mango]         |[[apple, banana, orange], [grape, mango]]                     |
|3  |[kiwi, pear, peach]    |[[apple, banana, orange], [grape, mango], [kiwi, pear, peach]]|
+---+-----------------------+--------------------------------------------------------------+