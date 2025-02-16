from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col


# For second argument of config in the SparkSession in main function go to mvnrepository.com, search for
# "spark-sql-kafka", click on the spark version that you use and copy the groupId and artifactId, for the third argument
# search hadoop-aws, for the 4-th one aws-java-sdk
def main():
    spark = (SparkSession.builder
             .appName("SmartCityStreaming")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
             .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate())

    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("snapshot", StringType(), True),
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", DoubleType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")  # Or "latest" if you only need new messages
                .option("failOnDataLoss", "false")  # Prevents failure if offsets change
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
                )

    def stream_writer(input: DataFrame, checkPointFolder, output):
        return (input.writeStream
                .format("parquet")
                .option("checkpointLocation", checkPointFolder)
                .option("path", output)
                .outputMode("append")
                .start())

    vehicleDF = read_kafka_topic("vehicle_data", vehicleSchema).alias("vehicle")
    gpsDF = read_kafka_topic("gps_data", gpsSchema).alias("gps")
    trafficDF = read_kafka_topic("traffic_data", trafficSchema).alias("traffic")
    weatherDF = read_kafka_topic("weather_data", weatherSchema).alias("weather")
    emergencyDF = read_kafka_topic("emergency_data", emergencySchema).alias("emergency")

    # join all the dfs with id and timestamp

    # rava-spark-streaming-data is the name of bucket in S3
    query1 = stream_writer(vehicleDF, "s3a://rava-spark-streaming-data/checkpoint/vehicle_data",
                           "s3a://rava-spark-streaming-data/data/vehicle_data")
    query2 = stream_writer(gpsDF, "s3a://rava-spark-streaming-data/checkpoint/gps_data",
                           "s3a://rava-spark-streaming-data/data/gps_data")
    query3 = stream_writer(trafficDF, "s3a://rava-spark-streaming-data/checkpoint/traffic_data",
                           "s3a://rava-spark-streaming-data/data/traffic_data")
    query4 = stream_writer(weatherDF, "s3a://rava-spark-streaming-data/checkpoint/weather_data",
                           "s3a://rava-spark-streaming-data/data/weather_data")
    query5 = stream_writer(emergencyDF, "s3a://rava-spark-streaming-data/checkpoint/emergency_data",
                           "s3a://rava-spark-streaming-data/data/emergency_data")

    # To run all queries in parallel
    query5.awaitTermination()


if __name__ == "__main__":
    main()
