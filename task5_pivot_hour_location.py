from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_timestamp, hour

# Start Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Analysis - Task 5") \
    .getOrCreate()

# Load the data
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Convert to timestamp type and extract hour
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
df = df.withColumn("hour_of_day", hour(col("timestamp")))

# Create pivot table: rows = location, columns = hour_of_day, values = avg temperature
pivot_df = df.groupBy("location") \
    .pivot("hour_of_day", list(range(24))) \
    .agg(avg("temperature"))

# Show the pivot table
pivot_df.show(truncate=False)

# Save to CSV
pivot_df.coalesce(1).write.option("header", "true").csv("task5_output.csv")
