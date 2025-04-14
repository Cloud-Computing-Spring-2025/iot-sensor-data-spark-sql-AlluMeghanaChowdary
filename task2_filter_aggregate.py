from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Analysis - Task 2") \
    .getOrCreate()

# Load CSV (assuming sensor_data.csv is in your working directory)
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Register Temp View
df.createOrReplaceTempView("sensor_readings")

# --------------------------
# PART A: Filtering
# --------------------------

# Filter rows where temperature is < 18 or > 30 (out-of-range)
out_of_range_df = df.filter((col("temperature") < 18) | (col("temperature") > 30))
out_of_range_count = out_of_range_df.count()

# Filter rows where temperature is between 18 and 30 (inclusive)
in_range_df = df.filter((col("temperature") >= 18) & (col("temperature") <= 30))
in_range_count = in_range_df.count()

print(f"Out-of-range rows: {out_of_range_count}")
print(f"In-range rows: {in_range_count}")

# --------------------------
# PART B: Aggregation
# --------------------------

# Group by location and calculate average temperature and humidity
agg_df = df.groupBy("location") \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    ) \
    .orderBy(col("avg_temperature").desc())  # Sort by hottest location

# Show results
agg_df.show()

# --------------------------
# Save result to CSV
# --------------------------
agg_df.coalesce(1).write.option("header", "true").csv("task2_output.csv")
