from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, DoubleType

# spark = (SparkSession.builder
#     .appName("driver-performance-aggregator")
#     .config("spark.jars.packages",
#             "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")  # ← this is the missing connector
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
#     .getOrCreate())

# spark = (SparkSession.builder
#     .appName("driver-performance-aggregator")
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
#     .getOrCreate())

spark = (SparkSession.builder
    .appName("driver-performance-aggregator")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN") # for reucing logs noise


schema = StructType() \
    .add("deviceID", StringType()) \
    .add("SpeedLimit", DoubleType()) \
    .add("timestamp", StringType())



# Read GPS stream with watermark
gps = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "broker:29092")
       .option("subscribe", "gps_data")
       .option("startingOffsets", "earliest")
       .load()
       .selectExpr("CAST(value AS STRING) as json")
       .select(F.from_json("json", "deviceId STRING, speed DOUBLE, timestamp STRING").alias("d"))
       .select("d.*")
       .withColumn("event_time", F.to_timestamp("timestamp"))
       .withWatermark("event_time", "5 minutes"))


gps_debug = gps.withColumn("DEBUG_TAG", F.lit("📡 GPS RAW"))
gps_debug.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

# Read speed limit stream with watermark
speed_limits = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("subscribe", "optimized_routes")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .withColumn("d", F.from_json("json", schema))
    .select(
        F.col("d.deviceID").alias("deviceId_sl"),
        F.col("d.SpeedLimit"),
        F.col("d.timestamp").alias("Timestamp")
    )
    .withColumn("event_time_speed", F.to_timestamp("Timestamp"))
    .withWatermark("event_time_speed", "5 minutes"))


sl_debug = speed_limits.withColumn("DEBUG_TAG", F.lit("🛣️ SPEEDLIMIT RAW"))
sl_debug.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()


# Read driver alerts stream
alerts = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "broker:29092")
          .option("subscribe", "driver_alerts")
          .option("startingOffsets", "earliest")
          .load()
          .selectExpr("CAST(value AS STRING) as json")
          .select(F.from_json("json", "deviceId STRING, action STRING, target_speed DOUBLE, ts LONG").alias("d"))
          .select("d.*")
          .withColumn("event_time", F.to_timestamp(F.col("ts") / 1000)))



# Add UNIX timestamp columns
gps = gps.withColumn("event_time_unix", F.unix_timestamp("event_time"))
speed_limits = speed_limits.withColumn("event_time_speed_unix", F.unix_timestamp("event_time_speed"))

# Join with time tolerance of 1 second
joined = gps.join(
    speed_limits,
    on=(
        (gps.deviceId == speed_limits.deviceId_sl) &
        (F.col("event_time").between(
            F.col("event_time_speed") - F.expr("INTERVAL 5 MINUTES"),
            F.col("event_time_speed") + F.expr("INTERVAL 5 MINUTES")
        ))
    ),
    how="left"
)

# Add debug tag
joined.withColumn("DEBUG_TAG", F.lit("🚦 JOINED ROW")) \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()


# Aggregate metrics
# metrics = (joined
#            .withColumn("over", F.when(F.col("speed") > F.col("SpeedLimit"), 1).otherwise(0))
#            .groupBy(
#                F.window("event_time", "1 hour").alias("w"),
#                gps.deviceId)
#            .agg(
#                F.avg(F.col("speed") - F.col("SpeedLimit")).alias("avg_speed_over_limit"),
#                F.sum("over").alias("overspeed_events"),
#                F.max(F.col("speed") - F.col("SpeedLimit")).alias("max_speed_over_limit"))
#             .selectExpr(
#                 "deviceId",
#                 "concat_ws('/', date_format(w.start, \"yyyy-MM-dd'T'HH:mm:ss\"), date_format(w.end, \"yyyy-MM-dd'T'HH:mm:ss\")) as period",
#                 "avg_speed_over_limit",
#                 "overspeed_events",
#                 "max_speed_over_limit"
#             )
#         )

metrics = (joined
           .filter(F.col("SpeedLimit").isNotNull())
           .withColumn("over", F.when(F.col("speed") > F.col("SpeedLimit"), 1).otherwise(0))
           .groupBy(
               F.window("event_time", "1 hour").alias("w"),
               "deviceId"  # Now safe to use
           )
           .agg(
               F.avg(F.col("speed") - F.col("SpeedLimit")).alias("avg_speed_over_limit"),
               F.sum("over").alias("overspeed_events"),
               F.max(F.col("speed") - F.col("SpeedLimit")).alias("max_speed_over_limit")
           )
           .selectExpr(
               "deviceId",
               "concat_ws('/', date_format(w.start, \"yyyy-MM-dd'T'HH:mm:ss\"), date_format(w.end, \"yyyy-MM-dd'T'HH:mm:ss\")) as period",
               "avg_speed_over_limit",
               "overspeed_events",
               "max_speed_over_limit"
           )
)




# Add debug column
metrics_with_debug = metrics.withColumn("DEBUG_TAG", F.lit("📊 FINAL METRICS"))

# ✅ Write to console (for monitoring)
metrics_with_debug.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

# ✅ Write to Kafka (drop debug tag before sending)
metrics_with_debug.drop("DEBUG_TAG") \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("topic", "driver_performance_metrics") \
    .option("checkpointLocation", "/tmp/chk_driver_perf") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
