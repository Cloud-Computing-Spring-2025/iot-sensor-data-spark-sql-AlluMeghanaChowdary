from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Data").getOrCreate()

# Load CSV file
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Create temporary view
df.createOrReplaceTempView("sensor_readings")

# Show first 5 rows
df.show(5)

# Total number of records
print("Total Records:", df.count())

# Distinct locations
spark.sql("SELECT DISTINCT location FROM sensor_readings").show()

# Save DataFrame to CSV
df.write.csv("task1_output.csv", header=True)
