from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql import DataFrame
import os

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()

    print(f'Key: {configuration.get("AWS_ACCESS_KEY")}!!!!!!')

    spark.sparkContext.setLogLevel('WARN')

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
        StructField("fuelType", StringType(), True),
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("cameraId", StringType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("incidentType", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col('value'), schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
                )

    vehicleDF = read_kafka_topic("vehicle_data", vehicleSchema).alias("vehicle")
    gpsDF = read_kafka_topic("gps_data", gpsSchema).alias("gps")
    trafficDF = read_kafka_topic("traffic_data", trafficSchema).alias("traffic")
    weatherDF = read_kafka_topic("weather_data", weatherSchema).alias("weather")
    emergencyDF = read_kafka_topic("emergency_data", emergencySchema).alias("emergency")


    def streamWriter(input: DataFrame, checkpointFolder, output):
        if checkpointFolder is None or output is None:
            raise ValueError("Checkpoint folder or output path is null!!!!!!!!!!!!!")
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start()
                )
    
    query1 = streamWriter(vehicleDF, 's3a://spark-streaming-data11/checkpoints/vehicle_data', 's3a://spark-streaming-data11/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://spark-streaming-data11/checkpoints/gps_data', 's3a://spark-streaming-data11/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://spark-streaming-data11/checkpoints/traffic_data', 's3a://spark-streaming-data11/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://spark-streaming-data11/checkpoints/weather_data', 's3a://spark-streaming-data11/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://spark-streaming-data11/checkpoints/emergency_data', 's3a://spark-streaming-data11/data/emergency_data')

    query5.awaitTermination()




if __name__ == "__main__":
    main()
