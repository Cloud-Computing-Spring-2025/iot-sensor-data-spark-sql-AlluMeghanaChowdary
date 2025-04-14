from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, avg

# Start Spark sessionimestamp")))

# Register temp view (updated with timestamp and hour)
df.createOrReplaceTempView("sensor_readings")

# Group by hour and calculate average temperature
spark = SparkSession.builder \
    .appName("IoT Sensor Analysis - Task 3") \
    .getOrCreate()

# Load the CSV (same as before)
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Convert timestamp column to proper TimestampType
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Extract hour from timestamp
df = df.withColumn("hour_of_day", hour(col("t
avg_temp_by_hour = df.groupBy("hour_of_day") \
    .agg(avg("temperature").alias("avg_temp")) \
    .orderBy("hour_of_day")

# Show results
avg_temp_by_hour.show(24)

# Save to CSV
avg_temp_by_hour.coalesce(1).write.option("header", "true").csv("task3_output.csv")
