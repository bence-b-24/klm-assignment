from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from utils import move_local_files, move_hdfs_files, get_spark_session


layer = "staging"
source_file_path = sys.argv[2]
target_path = f"{sys.argv[3]}/{layer}/"
catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"
archive_path = f"{sys.argv[4]}/bookings/"

spark = get_spark_session(app_name="extract_bookings", catalog_name=catalog_name, warehouse_path=sys.argv[3])

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

df_flights.write.format("parquet").mode("overwrite").save(target_path + "flights")
df_flights_passengers_asn.write.format("parquet").mode("overwrite").save(target_path + "flights_passengers_asn")

if catalog_name == "hdfs_catalog":
    move_hdfs_files(spark, source_file_path, archive_path)
else:
    move_local_files(source_file_path, archive_path)

spark.stop()