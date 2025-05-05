from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from py4j.java_gateway import java_import
import os
from pyspark.sql.utils import AnalysisException


layer = "staging"
source_file_path = sys.argv[2]
target_path = f"{sys.argv[3]}/{layer}/"
catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"
archive_path = f"{sys.argv[4]}/bookings/"

spark = SparkSession.builder.appName("extract_bookings").master("spark://spark-master:7077").getOrCreate()

def move_local_files(source_path, dest_path):
    os.makedirs(dest_path, exist_ok=True)
    for filename in os.listdir(source_path):
            src = os.path.join(source_path, filename)
            dst = os.path.join(dest_path, filename)
            os.rename(src, dst)
    print(f"Moved files from {source_path} to {dest_path}.")
    
def move_hdfs_files(spark, source_path, dest_path):
    sc = spark.sparkContext
    java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._jvm, "org.apache.hadoop.fs.Path")
    java_import(sc._jvm, "java.net.URI")

    conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.FileSystem.get(sc._jvm.URI(source_path), conf)

    src_path = sc._jvm.Path(source_path)
    dst_path = sc._jvm.Path(dest_path)

    if not fs.exists(dst_path):
        fs.mkdirs(dst_path)

    for file_status in fs.listStatus(src_path):
        file_path = file_status.getPath()
        dest_file_path = dst_path.suffix("/" + file_path.getName())
        fs.rename(file_path, dest_file_path)

    print(f"Moved files from {source_path} to {dest_path}.")

def extract_passengers(df: DataFrame) -> DataFrame:
    df_passengers = df.withColumn("passengers", F.explode("event.DataElement.travelrecord.passengersList"))
    df_passengers = df_passengers.select("passengers.*")
    df_passengers = df_passengers.dropDuplicates(["uci"])
    return df_passengers

def extract_flights(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df_flights = (df.withColumn("flights", F.explode("event.DataElement.travelrecord.productsList"))
                .withColumn("passengers", F.explode("event.DataElement.travelrecord.passengersList"))
                )
    df_flights = df_flights.select("timestamp", "passengers.uci", "flights.bookingStatus", "flights.flight.*")
    df_flights = df_flights.withColumn("flight_id", F.md5(F.concat_ws("|", "arrivalDate", "departureDate", "originAirport", "destinationAirport", "operatingAirline")))
    df_flights = df_flights.withColumn("arrivalDate", F.to_timestamp(F.col("arrivalDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    df_flights = df_flights.withColumn("departureDate", F.to_timestamp(F.col("departureDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    df_flights_passengers_asn = df_flights.select(
                                                F.col("uci"),
                                                F.col("bookingStatus"),
                                                F.col("flight_id"), 
                                                F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_ts")
                                                ).dropDuplicates().cache()

    df_flights = df_flights.drop("uci", "bookingStatus", "timestamp")
    df_flights = df_flights.dropDuplicates(["flight_id"]).cache()
    return df_flights, df_flights_passengers_asn

try:
    df = spark.read.format("json").option("inferSchema", "true").load(source_file_path)
except AnalysisException as e:
    print(f"[INFO] Source file not found at: {source_file_path}")
    spark.stop()
    sys.exit(0)

df_passengers = extract_passengers(df)
df_passengers.write.format("parquet").mode("overwrite").save(target_path + "passengers")



df_flights, df_flights_passengers_asn = extract_flights(df)

df_flights.coalesce(1).write.format("parquet").mode("overwrite").save(target_path + "flights")
df_flights_passengers_asn.coalesce(1).write.format("parquet").mode("overwrite").save(target_path + "flights_passengers_asn")

if catalog_name == "hdfs_catalog":
    move_hdfs_files(spark, source_file_path, archive_path)
else:
    move_local_files(source_file_path, archive_path)

spark.stop()