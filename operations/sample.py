from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("SelectExpr Examples") \
    .getOrCreate()

# Create sample data
data = [
    (1, "John", 30, 50000),
    (2, "Jane", 25, 60000),
    (3, "Bob", 35, 45000)
]
df = spark.createDataFrame(data, ["id", "name", "age", "salary"])

# Basic Examples:

# 1. Simple column selection with renaming
df.selectExpr("id", "name as employee_name").show()

# 2. Mathematical operations
df.selectExpr(
    "id",
    "name",
    "salary * 1.1 as salary_with_bonus",
    "salary / 12 as monthly_salary"
).show()

# 3. String operations
df.selectExpr(
    "id",
    "UPPER(name) as upper_name",
    "LOWER(name) as lower_name",
    "CONCAT(name, ' - ', CAST(age AS STRING)) as name_age"
).show()

# 4. Conditional expressions
df.selectExpr(
    "id",
    "name",
    "CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END as experience_level",
    "CASE WHEN salary >= 50000 THEN 'High' WHEN salary >= 40000 THEN 'Medium' ELSE 'Low' END as salary_band"
).show()

# 5. Using built-in SQL functions
df.selectExpr(
    "id",
    "name",
    "ROUND(salary, 2) as rounded_salary",
    "LENGTH(name) as name_length",
    "ABS(salary) as absolute_salary"
).show()

# 6. Multiple transformations
df.selectExpr(
    "*",  # Select all original columns
    "salary * 0.1 as bonus",
    "age + 5 as projected_age"
).show()

# 7. Aggregations (when used with groupBy)
df.groupBy("age").agg(
    expr("COUNT(*) as count"),
    expr("SUM(salary) as total_salary"),
    expr("AVG(salary) as avg_salary")
).show()

# 8. Using regular expressions
df.selectExpr(
    "id",
    "REGEXP_REPLACE(name, '[aeiou]', '*') as name_without_vowels"
).show()

# 9. Date functions (assuming we add a date column)
df_with_date = df.withColumn("join_date", current_date())
df_with_date.selectExpr(
    "id",
    "name",
    "DATE_ADD(join_date, 90) as probation_end_date",
    "MONTH(join_date) as join_month",
    "YEAR(join_date) as join_year"
).show()

# 10. Complex conditions
df.selectExpr(
    "id",
    "name",
    "CASE WHEN age < 30 AND salary > 50000 THEN 'Young High Earner' " +
    "WHEN age >= 30 AND salary > 50000 THEN 'Experienced High Earner' " +
    "ELSE 'Standard' END as employee_category"
).show()

"""
+---+-------------+
| id|employee_name|
+---+-------------+
|  1|         John|
|  2|         Jane|
|  3|          Bob|
+---+-------------+

+---+----+-----------------+-----------------+
| id|name|salary_with_bonus|   monthly_salary|
+---+----+-----------------+-----------------+
|  1|John|          55000.0|4166.666666666667|
|  2|Jane|          66000.0|           5000.0|
|  3| Bob|          49500.0|           3750.0|
+---+----+-----------------+-----------------+

+---+----------+----------+---------+
| id|upper_name|lower_name| name_age|
+---+----------+----------+---------+
|  1|      JOHN|      john|John - 30|
|  2|      JANE|      jane|Jane - 25|
|  3|       BOB|       bob| Bob - 35|
+---+----------+----------+---------+

+---+----+----------------+-----------+
| id|name|experience_level|salary_band|
+---+----+----------------+-----------+
|  1|John|          Junior|       High|
|  2|Jane|          Junior|       High|
|  3| Bob|          Senior|     Medium|
+---+----+----------------+-----------+

+---+----+--------------+-----------+---------------+
| id|name|rounded_salary|name_length|absolute_salary|
+---+----+--------------+-----------+---------------+
|  1|John|         50000|          4|          50000|
|  2|Jane|         60000|          4|          60000|
|  3| Bob|         45000|          3|          45000|
+---+----+--------------+-----------+---------------+

+---+----+---+------+------+-------------+
| id|name|age|salary| bonus|projected_age|
+---+----+---+------+------+-------------+
|  1|John| 30| 50000|5000.0|           35|
|  2|Jane| 25| 60000|6000.0|           30|
|  3| Bob| 35| 45000|4500.0|           40|
+---+----+---+------+------+-------------+

+---+-----+------------+----------+
|age|count|total_salary|avg_salary|
+---+-----+------------+----------+
| 30|    1|       50000|   50000.0|
| 25|    1|       60000|   60000.0|
| 35|    1|       45000|   45000.0|
+---+-----+------------+----------+

+---+-------------------+
| id|name_without_vowels|
+---+-------------------+
|  1|               J*hn|
|  2|               J*n*|
|  3|                B*b|
+---+-------------------+

+---+----+------------------+----------+---------+
| id|name|probation_end_date|join_month|join_year|
+---+----+------------------+----------+---------+
|  1|John|        2025-05-24|         2|     2025|
|  2|Jane|        2025-05-24|         2|     2025|
|  3| Bob|        2025-05-24|         2|     2025|
+---+----+------------------+----------+---------+

+---+----+-----------------+
| id|name|employee_category|
+---+----+-----------------+
|  1|John|         Standard|
|  2|Jane|Young High Earner|
|  3| Bob|         Standard|
+---+----+-----------------+

"""