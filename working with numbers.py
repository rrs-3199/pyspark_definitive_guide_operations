from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Number Operations") \
    .getOrCreate()

# Create sample DataFrame
data = [
    (1, 10.5, -5, 100),
    (2, 15.7, 3, 200),
    (3, 20.3, -8, 300),
    (4, None, 7, 400)
]
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("float_num", DoubleType(), True),
    StructField("int_num", IntegerType(), True),
    StructField("base_value", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

# 1. Basic Mathematical Operations
df_math = df.select(
    "id",
    "float_num",
    (col("float_num") + 2).alias("addition"),
    (col("float_num") - 2).alias("subtraction"),
    (col("float_num") * 2).alias("multiplication"),
    (col("float_num") / 2).alias("division"),
    (col("int_num") % 2).alias("modulo")
)

# 2. Rounding Functions
df_round = df.select(
    "float_num",
    round("float_num", 1).alias("rounded"),
    ceil("float_num").alias("ceiling"),
    floor("float_num").alias("floor"),
    bround("float_num", 1).alias("bank_rounding")
)

# 3. Absolute and Sign Functions
df_abs = df.select(
    "int_num",
    abs("int_num").alias("absolute_value"),
    signum("int_num").alias("sign")
)

# 4. Power and Square Root
df_power = df.select(
    "base_value",
    pow("base_value", 2).alias("squared"),
    sqrt("base_value").alias("square_root"),
    exp("base_value").alias("exponential"),
    log("base_value").alias("natural_log")
)

# 5. Trigonometric Functions
df_trig = df.select(
    "float_num",
    sin("float_num").alias("sine"),
    cos("float_num").alias("cosine"),
    tan("float_num").alias("tangent")
)

# 6. Statistical Functions
df_stats = df.select(
    mean("float_num").alias("mean"),
    stddev("float_num").alias("std_dev"),
    variance("float_num").alias("variance"),
    kurtosis("float_num").alias("kurtosis"),
    skewness("float_num").alias("skewness")
)

# 7. Aggregation Functions
df_agg = df.agg(
    sum("float_num").alias("total"),
    avg("float_num").alias("average"),
    min("float_num").alias("minimum"),
    max("float_num").alias("maximum"),
    count("float_num").alias("count")
)

# 8. Window Functions for Running Calculations
from pyspark.sql.window import Window

window_spec = Window.orderBy("id")
df_window = df.select(
    "id",
    "float_num",
    sum("float_num").over(window_spec).alias("running_total"),
    avg("float_num").over(window_spec).alias("running_avg")
)

# 9. Handling Null Values
df_null = df.select(
    "float_num",
    coalesce("float_num", lit(0.0)).alias("null_to_zero"),
    when(col("float_num").isNull(), 0.0)
    .otherwise(col("float_num")).alias("null_handled")
)

# 10. Custom Expressions using expr()
df_expr = df.selectExpr(
    "id",
    "float_num",
    "CASE WHEN float_num > 15 THEN 'High' ELSE 'Low' END as category",
    "POWER(float_num, 2) as custom_power"
)

# Show results
print("Basic Math Operations:")
df_math.show()

print("\nRounding Functions:")
df_round.show()

print("\nAbsolute and Sign Functions:")
df_abs.show()

print("\nPower and Square Root:")
df_power.show()

print("\nTrigonometric Functions:")
df_trig.show()

print("\nStatistical Functions:")
df_stats.show()

print("\nAggregation Functions:")
df_agg.show()

print("\nWindow Functions:")
df_window.show()

print("\nNull Handling:")
df_null.show()

print("\nCustom Expressions:")
df_expr.show()

# Format numbers as strings
df.select(
    format_number("float_num", 2).alias("formatted_number")
)

"""
Basic Math Operations:
+---+---------+--------+-----------+--------------+--------+------+
| id|float_num|addition|subtraction|multiplication|division|modulo|
+---+---------+--------+-----------+--------------+--------+------+
|  1|     10.5|    12.5|        8.5|          21.0|    5.25|    -1|
|  2|     15.7|    17.7|       13.7|          31.4|    7.85|     1|
|  3|     20.3|    22.3|       18.3|          40.6|   10.15|     0|
|  4|     NULL|    NULL|       NULL|          NULL|    NULL|     1|
+---+---------+--------+-----------+--------------+--------+------+


Rounding Functions:
+---------+-------+-------+-----+-------------+
|float_num|rounded|ceiling|floor|bank_rounding|
+---------+-------+-------+-----+-------------+
|     10.5|   10.5|     11|   10|         10.5|
|     15.7|   15.7|     16|   15|         15.7|
|     20.3|   20.3|     21|   20|         20.3|
|     NULL|   NULL|   NULL| NULL|         NULL|
+---------+-------+-------+-----+-------------+


Absolute and Sign Functions:
+-------+--------------+----+
|int_num|absolute_value|sign|
+-------+--------------+----+
|     -5|             5|-1.0|
|      3|             3| 1.0|
|     -8|             8|-1.0|
|      7|             7| 1.0|
+-------+--------------+----+


Power and Square Root:
+----------+--------+------------------+--------------------+-----------------+
|base_value| squared|       square_root|         exponential|      natural_log|
+----------+--------+------------------+--------------------+-----------------+
|       100| 10000.0|              10.0|2.688117141816135...|4.605170185988092|
|       200| 40000.0|14.142135623730951|7.225973768125749E86|5.298317366548036|
|       300| 90000.0|17.320508075688775|1.942426395241255...|5.703782474656201|
|       400|160000.0|              20.0|5.221469689764144...|5.991464547107982|
+----------+--------+------------------+--------------------+-----------------+


Trigonometric Functions:
+---------+--------------------+--------------------+--------------------+
|float_num|                sine|              cosine|             tangent|
+---------+--------------------+--------------------+--------------------+
|     10.5|   -0.87969575997167|-0.47553692799599256|  1.8498999934219273|
|     15.7|0.007963183785937343| -0.9999682933493399|-0.00796343627982...|
|     20.3|  0.9927664058359071| 0.12006191504242673|   8.268787029468001|
|     NULL|                NULL|                NULL|                NULL|
+---------+--------------------+--------------------+--------------------+


Statistical Functions:
+----+-----------------+------------------+--------+--------------------+
|mean|          std_dev|          variance|kurtosis|            skewness|
+----+-----------------+------------------+--------+--------------------+
|15.5|4.903060268852506|24.040000000000003|    -1.5|-0.07481288986687265|
+----+-----------------+------------------+--------+--------------------+


Aggregation Functions:
+-----+-------+-------+-------+-----+
|total|average|minimum|maximum|count|
+-----+-------+-------+-------+-----+
| 46.5|   15.5|   10.5|   20.3|    3|
+-----+-------+-------+-------+-----+


Window Functions:
+---+---------+-------------+-----------+
| id|float_num|running_total|running_avg|
+---+---------+-------------+-----------+
|  1|     10.5|         10.5|       10.5|
|  2|     15.7|         26.2|       13.1|
|  3|     20.3|         46.5|       15.5|
|  4|     NULL|         46.5|       15.5|
+---+---------+-------------+-----------+


Null Handling:
+---------+------------+------------+
|float_num|null_to_zero|null_handled|
+---------+------------+------------+
|     10.5|        10.5|        10.5|
|     15.7|        15.7|        15.7|
|     20.3|        20.3|        20.3|
|     NULL|         0.0|         0.0|
+---------+------------+------------+


Custom Expressions:
+---+---------+--------+------------------+
| id|float_num|category|      custom_power|
+---+---------+--------+------------------+
|  1|     10.5|     Low|            110.25|
|  2|     15.7|    High|246.48999999999998|
|  3|     20.3|    High|412.09000000000003|
|  4|     NULL|     Low|              NULL|
+---+---------+--------+------------------+
"""