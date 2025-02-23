from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create SparkSession
spark = SparkSession.builder \
    .appName("DataFrame Transformations") \
    .getOrCreate()

# Sample DataFrame
data = [
    (1, "John", 30, "Sales", 50000, "NY"),
    (2, "Alice", 25, "IT", 60000, "SF"),
    (3, "Bob", 35, "Sales", 45000, "LA"),
    (4, "Mary", 28, "IT", 55000, "NY")
]
df = spark.createDataFrame(data, ["id", "name", "age", "dept", "salary", "city"])

# 1. Select and Column Operations
df_select = df.select(
    col("id"),
    upper(col("name")).alias("name_upper"),
    (col("salary") * 1.1).alias("new_salary")
)

# 2. Filter Operations
df_filter = df.filter(
    (col("age") > 25) &
    (col("dept") == "Sales")
)

# 3. GroupBy and Aggregations
df_agg = df.groupBy("dept").agg(
    count("id").alias("emp_count"),
    avg("salary").alias("avg_salary"),
    max("age").alias("max_age")
)

# 4. Window Functions
window_spec = Window.partitionBy("dept").orderBy("salary")
df_window = df.withColumn(
    "rank", rank().over(window_spec)
).withColumn(
    "dept_avg_salary", avg("salary").over(Window.partitionBy("dept"))
)

# 5. Join Operations
other_data = [(1, "High"), (2, "Medium"), (3, "Low")]
df_rating = spark.createDataFrame(other_data, ["id", "rating"])

df_joined = df.join(
    df_rating,
    on="id",
    how="left"
)

# 6. Adding and Modifying Columns
df_new_cols = df.withColumn(
    "salary_category",
    when(col("salary") > 55000, "High")
    .when(col("salary") > 45000, "Medium")
    .otherwise("Low")
).withColumn(
    "full_info",
    concat_ws(" - ", col("name"), col("dept"), col("city"))
)

# 7. Sorting
df_sorted = df.orderBy(
    desc("salary"),
    asc("age")
)

# 8. Handling NULL values
df_null = df.na.fill({
    "salary": 0,
    "city": "Unknown"
}).na.drop(subset=["name"])

# 9. Pivot and Unpivot
df_pivot = df.groupBy("city").pivot("dept").agg(count("id"))

# 10. String Operations
df_string = df.select(
    lower(col("name")).alias("name_lower"),
    length(col("name")).alias("name_length"),
    concat(col("name"), lit(" - "), col("dept")).alias("name_dept")
)

# 11. Date Operations (assuming we add a date column)
df_with_date = df.withColumn("join_date", current_date())
df_dates = df_with_date.select(
    col("name"),
    date_add(col("join_date"), 90).alias("probation_end"),
    months_between(current_date(), col("join_date")).alias("months_employed")
)

# 12. Mathematical Operations
df_math = df.select(
    col("name"),
    round(col("salary") / 12, 2).alias("monthly_salary"),
    pow(col("age"), 2).alias("age_squared")
)

# 13. Sampling and Limiting
df_sample = df.sample(fraction=0.5, seed=42)
df_limit = df.limit(2)

# 14. Drop Duplicates
df_distinct = df.dropDuplicates(["dept", "city"])

# 15. Rename Columns
df_renamed = df.withColumnRenamed("dept", "department")

# Show results
print("Original DataFrame:")
df.show()

print("\nSelect and Column Operations:")
df_select.show()

print("\nFilter Operations:")
df_filter.show()

print("\nGroupBy and Aggregations:")
df_agg.show()

print("\nWindow Functions:")
df_window.show()

print("\nJoin Operations:")
df_joined.show()

print("\nNew Columns:")
df_new_cols.show()


"""
Original DataFrame:
+---+-----+---+-----+------+----+
| id| name|age| dept|salary|city|
+---+-----+---+-----+------+----+
|  1| John| 30|Sales| 50000|  NY|
|  2|Alice| 25|   IT| 60000|  SF|
|  3|  Bob| 35|Sales| 45000|  LA|
|  4| Mary| 28|   IT| 55000|  NY|
+---+-----+---+-----+------+----+


Select and Column Operations:
+---+----------+-----------------+
| id|name_upper|       new_salary|
+---+----------+-----------------+
|  1|      JOHN|55000.00000000001|
|  2|     ALICE|          66000.0|
|  3|       BOB|49500.00000000001|
|  4|      MARY|60500.00000000001|
+---+----------+-----------------+


Filter Operations:
+---+----+---+-----+------+----+
| id|name|age| dept|salary|city|
+---+----+---+-----+------+----+
|  1|John| 30|Sales| 50000|  NY|
|  3| Bob| 35|Sales| 45000|  LA|
+---+----+---+-----+------+----+


GroupBy and Aggregations:
+-----+---------+----------+-------+
| dept|emp_count|avg_salary|max_age|
+-----+---------+----------+-------+
|Sales|        2|   47500.0|     35|
|   IT|        2|   57500.0|     28|
+-----+---------+----------+-------+


Window Functions:
+---+-----+---+-----+------+----+----+---------------+
| id| name|age| dept|salary|city|rank|dept_avg_salary|
+---+-----+---+-----+------+----+----+---------------+
|  4| Mary| 28|   IT| 55000|  NY|   1|        57500.0|
|  2|Alice| 25|   IT| 60000|  SF|   2|        57500.0|
|  3|  Bob| 35|Sales| 45000|  LA|   1|        47500.0|
|  1| John| 30|Sales| 50000|  NY|   2|        47500.0|
+---+-----+---+-----+------+----+----+---------------+


Join Operations:
+---+-----+---+-----+------+----+------+
| id| name|age| dept|salary|city|rating|
+---+-----+---+-----+------+----+------+
|  1| John| 30|Sales| 50000|  NY|  High|
|  2|Alice| 25|   IT| 60000|  SF|Medium|
|  3|  Bob| 35|Sales| 45000|  LA|   Low|
|  4| Mary| 28|   IT| 55000|  NY|  NULL|
+---+-----+---+-----+------+----+------+


New Columns:
+---+-----+---+-----+------+----+---------------+-----------------+
| id| name|age| dept|salary|city|salary_category|        full_info|
+---+-----+---+-----+------+----+---------------+-----------------+
|  1| John| 30|Sales| 50000|  NY|         Medium|John - Sales - NY|
|  2|Alice| 25|   IT| 60000|  SF|           High|  Alice - IT - SF|
|  3|  Bob| 35|Sales| 45000|  LA|            Low| Bob - Sales - LA|
|  4| Mary| 28|   IT| 55000|  NY|         Medium|   Mary - IT - NY|
+---+-----+---+-----+------+----+---------------+-----------------+
"""