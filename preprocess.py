from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

s = SparkSession.builder.appName("hw4-preprocess").master("local[*]").getOrCreate()
s.sparkContext.setLogLevel("WARN")

d = s.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

d = d.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / lit(60.0)))

d.createOrReplaceTempView("trips")

r = s.sql(
    "SELECT company, COUNT(*) AS trip_count, "
    "ROUND(AVG(fare), 2) AS avg_fare, "
    "ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute "
    "FROM trips GROUP BY company ORDER BY trip_count DESC"
)

r.coalesce(1).write.mode("overwrite").json("processed_data")

r.show(truncate=False)

s.stop()
