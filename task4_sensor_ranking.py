from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Start Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Analysis - Task 4") \
    .getOrCreate()

# Load CSV
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Calculate average temperature per sensor
avg_temp_df = df.groupBy("sensor_id") \
    .agg(avg("temperature").alias("avg_temp"))

# Define window spec for ranking (descending order)
window_spec = Window.orderBy(col("avg_temp").desc())

# Apply RANK window function
ranked_df = avg_temp_df.withColumn("rank_temp", rank().over(window_spec))

# Show top 5 sensors by average temperature
top_5_sensors = ranked_df.orderBy("rank_temp").limit(5)
top_5_sensors.show()

# Save to CSV
top_5_sensors.coalesce(1).write.option("header", "true").csv("task4_output.csv")
