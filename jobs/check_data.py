from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataCheck").getOrCreate()

# Đọc dữ liệu vehicle_data
vehicle_df = spark.read.parquet('/tmp/data/vehicle_data')

# Hiển thị một số bản ghi
vehicle_df.show(5)

# Đọc dữ liệu gps_data
gps_df = spark.read.parquet('/tmp/data/gps_data')

# Hiển thị một số bản ghi
gps_df.show(5)