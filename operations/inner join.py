from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("InnerJoinExample").getOrCreate()

# Create sample DataFrames
employees = spark.createDataFrame([
    (1, "John", "Engineering"),
    (2, "Jane", "Marketing"),
    (3, "David", "Finance"),
    (4, "Anna", "HR")
], ["id", "name", "department"])

departments = spark.createDataFrame([
    ("Engineering", "Building A"),
    ("Marketing", "Building B"),
    ("Finance", "Building A"),
    ("Sales", "Building C")
], ["department", "location"])

# Perform inner join
result = employees.join(departments, on="department", how="inner")

# Show result
result.show()


"""
+-----------+---+-----+----------+
| department| id| name|  location|
+-----------+---+-----+----------+
|Engineering|  1| John|Building A|
|    Finance|  3|David|Building A|
|  Marketing|  2| Jane|Building B|
+-----------+---+-----+----------+
"""