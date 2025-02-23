from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("PySpark Expressions") \
    .getOrCreate()

# Create sample DataFrame
data = [
    (1, "John", 30, 50000, "IT", "2023-01-01"),
    (2, "Alice", 25, 60000, "HR", "2023-02-15"),
    (3, "Bob", 35, 45000, "IT", "2023-03-10"),
    (4, "Emma", 28, 55000, "Sales", "2023-04-20")
]

df = spark.createDataFrame(data, ["id", "name", "age", "salary", "dept", "join_date"])

# 1. String Expressions
df.select(
    col("name"),
    upper(col("name")).alias("upper_name"),
    lower(col("name")).alias("lower_name"),
    concat(col("name"), lit(" - "), col("dept")).alias("name_dept"),
    length(col("name")).alias("name_length"),
    substring(col("name"), 1, 2).alias("first_two_chars")
).show()

# 2. Mathematical Expressions
df.select(
    col("salary"),
    round(col("salary") * 1.1, 2).alias("salary_with_bonus"),
    sqrt(col("salary")).alias("sqrt_salary"),
    abs(col("salary")).alias("abs_salary"),
    pow(col("salary"), 2).alias("salary_squared")
).show()

# 3. Date Expressions
df.select(
    col("join_date"),
    date_format(col("join_date"), "MM/dd/yyyy").alias("formatted_date"),
    datediff(current_date(), col("join_date")).alias("days_employed"),
    add_months(col("join_date"), 3).alias("three_months_later"),
    year(col("join_date")).alias("join_year"),
    month(col("join_date")).alias("join_month")
).show()

# 4. Conditional Expressions
df.select(
    col("name"),
    col("age"),
    when(col("age") > 30, "Senior")
    .when(col("age") > 25, "Mid-level")
    .otherwise("Junior").alias("level"),

    # Case statement
    expr("""
        CASE 
            WHEN salary >= 55000 THEN 'High'
            WHEN salary >= 45000 THEN 'Medium'
            ELSE 'Low'
        END AS salary_band
    """)
).show()

# 5. Aggregate Expressions
df.groupBy("dept").agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

# 6. Window Functions
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("dept").orderBy("salary")

df.select(
    col("name"),
    col("dept"),
    col("salary"),
    rank().over(windowSpec).alias("salary_rank"),
    dense_rank().over(windowSpec).alias("dense_rank"),
    row_number().over(windowSpec).alias("row_number"),
    lag("salary", 1).over(windowSpec).alias("prev_salary"),
    lead("salary", 1).over(windowSpec).alias("next_salary")
).show()

# 7. Regular Expression Functions
df.select(
    col("name"),
    regexp_replace(col("name"), "[aeiou]", "*").alias("name_no_vowels"),
    regexp_extract(col("name"), "([A-Z])\w+", 1).alias("first_letter")
).show()

# 8. Type Casting Expressions
df.select(
    col("salary"),
    col("salary").cast("string").alias("salary_string"),
    col("join_date").cast("timestamp").alias("join_timestamp")
).show()

# 9. Complex Expressions
df.select(
    col("name"),
    col("salary"),
    when(col("dept") == "IT", col("salary") * 1.2)
    .when(col("dept") == "Sales", col("salary") * 1.1)
    .otherwise(col("salary")).alias("adjusted_salary"),

    expr("CASE WHEN age > 30 AND salary > 50000 THEN 'High Value' " +
         "WHEN age > 25 AND salary > 45000 THEN 'Mid Value' " +
         "ELSE 'Standard' END").alias("employee_category")
).show()


# 10. Custom UDF Expressions
def salary_category(salary):
    if salary >= 55000:
        return "High"
    elif salary >= 45000:
        return "Medium"
    return "Low"


# Register UDF
salary_category_udf = udf(salary_category, StringType())

df.select(
    col("name"),
    col("salary"),
    salary_category_udf(col("salary")).alias("salary_category")
).show()

"""
+-----+----------+----------+------------+-----------+---------------+
| name|upper_name|lower_name|   name_dept|name_length|first_two_chars|
+-----+----------+----------+------------+-----------+---------------+
| John|      JOHN|      john|   John - IT|          4|             Jo|
|Alice|     ALICE|     alice|  Alice - HR|          5|             Al|
|  Bob|       BOB|       bob|    Bob - IT|          3|             Bo|
| Emma|      EMMA|      emma|Emma - Sales|          4|             Em|
+-----+----------+----------+------------+-----------+---------------+

+------+-----------------+------------------+----------+--------------+
|salary|salary_with_bonus|       sqrt_salary|abs_salary|salary_squared|
+------+-----------------+------------------+----------+--------------+
| 50000|          55000.0|223.60679774997897|     50000|         2.5E9|
| 60000|          66000.0|244.94897427831782|     60000|         3.6E9|
| 45000|          49500.0|212.13203435596427|     45000|       2.025E9|
| 55000|          60500.0| 234.5207879911715|     55000|       3.025E9|
+------+-----------------+------------------+----------+--------------+

+----------+--------------+-------------+------------------+---------+----------+
| join_date|formatted_date|days_employed|three_months_later|join_year|join_month|
+----------+--------------+-------------+------------------+---------+----------+
|2023-01-01|    01/01/2023|          784|        2023-04-01|     2023|         1|
|2023-02-15|    02/15/2023|          739|        2023-05-15|     2023|         2|
|2023-03-10|    03/10/2023|          716|        2023-06-10|     2023|         3|
|2023-04-20|    04/20/2023|          675|        2023-07-20|     2023|         4|
+----------+--------------+-------------+------------------+---------+----------+

+-----+---+---------+-----------+
| name|age|    level|salary_band|
+-----+---+---------+-----------+
| John| 30|Mid-level|     Medium|
|Alice| 25|   Junior|       High|
|  Bob| 35|   Senior|     Medium|
| Emma| 28|Mid-level|       High|
+-----+---+---------+-----------+

+-----+--------------+------------+----------+----------+----------+
| dept|employee_count|total_salary|avg_salary|min_salary|max_salary|
+-----+--------------+------------+----------+----------+----------+
|   HR|             1|       60000|   60000.0|     60000|     60000|
|   IT|             2|       95000|   47500.0|     45000|     50000|
|Sales|             1|       55000|   55000.0|     55000|     55000|
+-----+--------------+------------+----------+----------+----------+

+-----+-----+------+-----------+----------+----------+-----------+-----------+
| name| dept|salary|salary_rank|dense_rank|row_number|prev_salary|next_salary|
+-----+-----+------+-----------+----------+----------+-----------+-----------+
|Alice|   HR| 60000|          1|         1|         1|       NULL|       NULL|
|  Bob|   IT| 45000|          1|         1|         1|       NULL|      50000|
| John|   IT| 50000|          2|         2|         2|      45000|       NULL|
| Emma|Sales| 55000|          1|         1|         1|       NULL|       NULL|
+-----+-----+------+-----------+----------+----------+-----------+-----------+

+-----+--------------+------------+
| name|name_no_vowels|first_letter|
+-----+--------------+------------+
| John|          J*hn|           J|
|Alice|         Al*c*|           A|
|  Bob|           B*b|           B|
| Emma|          Emm*|           E|
+-----+--------------+------------+

+------+-------------+-------------------+
|salary|salary_string|     join_timestamp|
+------+-------------+-------------------+
| 50000|        50000|2023-01-01 00:00:00|
| 60000|        60000|2023-02-15 00:00:00|
| 45000|        45000|2023-03-10 00:00:00|
| 55000|        55000|2023-04-20 00:00:00|
+------+-------------+-------------------+

+-----+------+-----------------+-----------------+
| name|salary|  adjusted_salary|employee_category|
+-----+------+-----------------+-----------------+
| John| 50000|          60000.0|        Mid Value|
|Alice| 60000|          60000.0|         Standard|
|  Bob| 45000|          54000.0|         Standard|
| Emma| 55000|60500.00000000001|        Mid Value|
+-----+------+-----------------+-----------------+

+-----+------+---------------+
| name|salary|salary_category|
+-----+------+---------------+
| John| 50000|         Medium|
|Alice| 60000|           High|
|  Bob| 45000|         Medium|
| Emma| 55000|           High|
+-----+------+---------------+
"""