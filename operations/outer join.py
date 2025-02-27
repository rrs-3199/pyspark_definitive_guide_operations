from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Sample data
employees_data = [
    (1, "John", "Engineering"),
    (2, "Jane", "Marketing"),
    (3, "David", "Finance")
]

departments_data = [
    ("Engineering", "Building A"),
    ("Marketing", "Building B"),
    ("HR", "Building C")
]

# Create DataFrames
employees = spark.createDataFrame(employees_data, ["id", "name", "dept"])
departments = spark.createDataFrame(departments_data, ["dept", "location"])

# Full outer join
full_join = employees.join(departments, on="dept", how="full")
full_join.show()

"""
+-----------+----+-----+----------+
|       dept|  id| name|  location|
+-----------+----+-----+----------+
|Engineering|   1| John|Building A|
|    Finance|   3|David|      NULL|
|         HR|NULL| NULL|Building C|
|  Marketing|   2| Jane|Building B|
+-----------+----+-----+----------+
"""