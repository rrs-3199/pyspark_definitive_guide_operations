from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Union Examples") \
    .getOrCreate()

# Create sample dataframes
df1 = spark.createDataFrame([
    (1, "John", 30),
    (2, "Alice", 25)
], ["id", "name", "age"])

df2 = spark.createDataFrame([
    (3, "Bob", 35),
    (4, "Mary", 28)
], ["id", "name", "age"])

# 1. Simple Union (requires same schema)
df_union = df1.union(df2)
print("Simple Union:")
df_union.show()

# 2. Union with unionAll (same as union)
df_union_all = df1.unionAll(df2)
print("\nUnion All:")
df_union_all.show()

# 3. Union with different column order
df3 = spark.createDataFrame([
    ("Charlie", 40, 5),
    ("David", 45, 6)
], ["name", "age", "id"])

# Align columns before union
df3_aligned = df3.select("id", "name", "age")
df_union_aligned = df1.union(df3_aligned)
print("\nUnion with aligned columns:")
df_union_aligned.show()

# 4. Union with distinct rows (remove duplicates)
df4 = spark.createDataFrame([
    (1, "John", 30),  # Duplicate row
    (5, "Eve", 32)
], ["id", "name", "age"])

df_union_distinct = df1.union(df4).distinct()
print("\nUnion with distinct rows:")
df_union_distinct.show()

# 5. Union multiple dataframes
df5 = spark.createDataFrame([
    (7, "Frank", 38)
], ["id", "name", "age"])

df_multi_union = df1.union(df2).union(df5)
# Alternative way:
# from functools import reduce
# dfs = [df1, df2, df5]
# df_multi_union = reduce(lambda x, y: x.union(y), dfs)

print("\nUnion of multiple dataframes:")
df_multi_union.show()

# 6. Union with different schemas (adding missing columns)
df6 = spark.createDataFrame([
    (8, "Grace", 42, "NY"),
    (9, "Henry", 33, "LA")
], ["id", "name", "age", "city"])

# Add missing column to df1 with null values
df1_with_city = df1.withColumn("city", lit(None))
df_union_schema = df1_with_city.union(df6)
print("\nUnion with different schemas:")
df_union_schema.show()

# 7. Union with type casting
df7 = spark.createDataFrame([
    (10, "Ian", "35"),  # age as string
    (11, "Jack", "38")
], ["id", "name", "age"])

# Cast age to integer before union
df7_casted = df7.withColumn("age", col("age").cast("integer"))
df_union_cast = df1.union(df7_casted)
print("\nUnion with type casting:")
df_union_cast.show()

# 8. Union with row ordering
df_union_ordered = df1.union(df2).orderBy("age")
print("\nUnion with ordering:")
df_union_ordered.show()

# 9. Union with aggregation
df_union_agg = df1.union(df2) \
    .groupBy("age") \
    .agg(count("id").alias("count"),
         collect_list("name").alias("names"))
print("\nUnion with aggregation:")
df_union_agg.show(truncate=False)

"""
Simple Union:
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1| John| 30|
|  2|Alice| 25|
|  3|  Bob| 35|
|  4| Mary| 28|
+---+-----+---+


Union All:
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1| John| 30|
|  2|Alice| 25|
|  3|  Bob| 35|
|  4| Mary| 28|
+---+-----+---+


Union with aligned columns:
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  1|   John| 30|
|  2|  Alice| 25|
|  5|Charlie| 40|
|  6|  David| 45|
+---+-------+---+


Union with distinct rows:
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1| John| 30|
|  2|Alice| 25|
|  5|  Eve| 32|
+---+-----+---+


Union of multiple dataframes:
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1| John| 30|
|  2|Alice| 25|
|  3|  Bob| 35|
|  4| Mary| 28|
|  7|Frank| 38|
+---+-----+---+


Union with different schemas:
+---+-----+---+----+
| id| name|age|city|
+---+-----+---+----+
|  1| John| 30|NULL|
|  2|Alice| 25|NULL|
|  8|Grace| 42|  NY|
|  9|Henry| 33|  LA|
+---+-----+---+----+


Union with type casting:
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1| John| 30|
|  2|Alice| 25|
| 10|  Ian| 35|
| 11| Jack| 38|
+---+-----+---+


Union with ordering:
+---+-----+---+
| id| name|age|
+---+-----+---+
|  2|Alice| 25|
|  4| Mary| 28|
|  1| John| 30|
|  3|  Bob| 35|
+---+-----+---+


Union with aggregation:
+---+-----+-------+
|age|count|names  |
+---+-----+-------+
|30 |1    |[John] |
|25 |1    |[Alice]|
|35 |1    |[Bob]  |
|28 |1    |[Mary] |
+---+-----+-------+
"""