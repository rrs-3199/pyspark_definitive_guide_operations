from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Boolean Examples") \
    .getOrCreate()

# Create sample DataFrame with boolean columns
data = [
    (1, "John", True, True, 25),
    (2, "Alice", False, True, 30),
    (3, "Bob", True, False, 35),
    (4, "Sarah", False, False, 28)
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("is_active", BooleanType(), True),
    StructField("has_subscription", BooleanType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

# 1. Basic Boolean Filtering
active_users = df.filter(col("is_active") == True)
inactive_users = df.filter(~col("is_active"))  # Using NOT operator

# 2. Multiple Boolean Conditions (AND, OR)
active_with_subscription = df.filter((col("is_active")) & (col("has_subscription")))
active_or_subscription = df.filter((col("is_active")) | (col("has_subscription")))

# 3. Creating Boolean Columns
df_with_bool = df.withColumn(
    "is_young",
    col("age") < 30
)

# 4. Boolean Operations with when/otherwise
df_with_status = df.withColumn(
    "status",
    when((col("is_active")) & (col("has_subscription")), "Premium")
    .when(col("is_active"), "Active")
    .when(col("has_subscription"), "Subscriber")
    .otherwise("Basic")
)

# 5. Aggregating Boolean Columns
bool_aggs = df.agg(
    sum(col("is_active").cast("int")).alias("total_active"),
    avg(col("is_active").cast("double")).alias("active_ratio"),
    sum(col("has_subscription").cast("int")).alias("total_subscriptions")
)

# 6. Complex Boolean Expressions
df_complex = df.withColumn(
    "special_status",
    (col("is_active") & col("has_subscription")) |
    (col("age") < 30)
)

# 7. Working with NULL values in boolean operations
df_with_null = df.withColumn(
    "nullable_bool",
    when(col("age") > 30, None)
    .otherwise(col("is_active"))
)

# 8. Boolean Array Operations
df_bool_array = df.withColumn(
    "bool_array",
    array(col("is_active"), col("has_subscription"))
).withColumn(
    "any_true",
    array_contains(col("bool_array"), True)
)

# 9. Comparing Boolean Columns
df_comparison = df.withColumn(
    "same_status",
    col("is_active") == col("has_subscription")
)

# Show results
print("Original DataFrame:")
df.show()

print("\nActive Users:")
active_users.show()

print("\nActive Users with Subscription:")
active_with_subscription.show()

print("\nDataFrame with Status:")
df_with_status.show()

print("\nBoolean Aggregations:")
bool_aggs.show()

print("\nComplex Boolean Operations:")
df_complex.show()

# 10. Common Boolean Functions and Operations
df_operations = df.select(
    col("id"),
    col("name"),
    col("is_active"),
    col("has_subscription"),
    # NOT operation
    (~col("is_active")).alias("is_not_active"),
    # AND operation
    (col("is_active") & col("has_subscription")).alias("is_premium"),
    # OR operation
    (col("is_active") | col("has_subscription")).alias("has_any_status"),
    # NAND operation
    (~(col("is_active") & col("has_subscription"))).alias("not_premium"),
    # NOR operation
    (~(col("is_active") | col("has_subscription"))).alias("no_status")
)

print("\nBoolean Operations:")
df_operations.show()

"""
Original DataFrame:
+---+-----+---------+----------------+---+
| id| name|is_active|has_subscription|age|
+---+-----+---------+----------------+---+
|  1| John|     true|            true| 25|
|  2|Alice|    false|            true| 30|
|  3|  Bob|     true|           false| 35|
|  4|Sarah|    false|           false| 28|
+---+-----+---------+----------------+---+


Active Users:
+---+----+---------+----------------+---+
| id|name|is_active|has_subscription|age|
+---+----+---------+----------------+---+
|  1|John|     true|            true| 25|
|  3| Bob|     true|           false| 35|
+---+----+---------+----------------+---+


Active Users with Subscription:
+---+----+---------+----------------+---+
| id|name|is_active|has_subscription|age|
+---+----+---------+----------------+---+
|  1|John|     true|            true| 25|
+---+----+---------+----------------+---+


DataFrame with Status:
+---+-----+---------+----------------+---+----------+
| id| name|is_active|has_subscription|age|    status|
+---+-----+---------+----------------+---+----------+
|  1| John|     true|            true| 25|   Premium|
|  2|Alice|    false|            true| 30|Subscriber|
|  3|  Bob|     true|           false| 35|    Active|
|  4|Sarah|    false|           false| 28|     Basic|
+---+-----+---------+----------------+---+----------+


Boolean Aggregations:
+------------+------------+-------------------+
|total_active|active_ratio|total_subscriptions|
+------------+------------+-------------------+
|           2|         0.5|                  2|
+------------+------------+-------------------+


Complex Boolean Operations:
+---+-----+---------+----------------+---+--------------+
| id| name|is_active|has_subscription|age|special_status|
+---+-----+---------+----------------+---+--------------+
|  1| John|     true|            true| 25|          true|
|  2|Alice|    false|            true| 30|         false|
|  3|  Bob|     true|           false| 35|         false|
|  4|Sarah|    false|           false| 28|          true|
+---+-----+---------+----------------+---+--------------+


Boolean Operations:
+---+-----+---------+----------------+-------------+----------+--------------+-----------+---------+
| id| name|is_active|has_subscription|is_not_active|is_premium|has_any_status|not_premium|no_status|
+---+-----+---------+----------------+-------------+----------+--------------+-----------+---------+
|  1| John|     true|            true|        false|      true|          true|      false|    false|
|  2|Alice|    false|            true|         true|     false|          true|       true|    false|
|  3|  Bob|     true|           false|        false|     false|          true|       true|    false|
|  4|Sarah|    false|           false|         true|     false|         false|       true|     true|
+---+-----+---------+----------------+-------------+----------+--------------+-----------+---------+

"""