from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from datetime import timedelta
from utils import get_spark_session


layer = "reporting"
table_name = "pasengers_by_country_dayofweek_season"

source_path = sys.argv[3] + "/normalized/"
target_path = f"{sys.argv[3]}/{layer}/{table_name}"

catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"

start_date = sys.argv[4] if sys.argv[4] != "none" else None
end_date = sys.argv[5] if sys.argv[5] != "none" else None

spark = get_spark_session(app_name="create_report", catalog_name=catalog_name, warehouse_path=sys.argv[3])

def add_hours_to_timestamp(timestamp_col, hours_col):
    if timestamp_col is not None and hours_col is not None:
        return timestamp_col + timedelta(hours=int(hours_col))
    return None

add_hours_udf = F.udf(add_hours_to_timestamp, returnType=T.TimestampType())

# Select only those bookings where the latest status is 'CONFIRMED'
df_flights_passengers_asn = spark.read.format("iceberg").load(f"{catalog_name}.normalized.flights_passengers_asn")
partition = Window.partitionBy("uci", "flight_id").orderBy(F.desc("event_ts"))
df_flights_passengers_asn = df_flights_passengers_asn.withColumn("rn", F.row_number().over(partition))
df_flights_passengers_asn = df_flights_passengers_asn.filter(F.col("rn") == 1).drop("rn")
df_flights_passengers_asn = df_flights_passengers_asn.filter(F.col("bookingStatus") == "CONFIRMED")

# Filter by date range if provided
if start_date is not None and end_date is not None:
    print(f"Filtering data between {start_date} and {end_date}")
    df_flights_passengers_asn = df_flights_passengers_asn.filter(F.col("event_ts").between(F.to_date(F.lit(start_date)), F.to_date(F.lit(end_date))))


# Select flights starting from the Netherlands, operated by KLM
df_flights = spark.read.format("iceberg").load(f"{catalog_name}.normalized.flights")
df_flights = df_flights.filter(F.col("originCountry") == F.lit("Netherlands")).filter(F.col("operatingAirline") == F.lit("KL"))
df_flights = df_flights.withColumn("adjustedDepartureDate", add_hours_udf("departureDate", "originTimezone"))
df_flights = df_flights.withColumn("dayOfWeek", F.dayofweek("adjustedDepartureDate"))
df_flights = df_flights.withColumn("season", F.when(F.month("adjustedDepartureDate").between(3, 5), "Spring")
                                        .when(F.month("adjustedDepartureDate").between(6, 8), "Summer")
                                        .when(F.month("adjustedDepartureDate").between(9, 11), "Autumn")
                                        .otherwise("Winter"))

df_flights = df_flights.join(df_flights_passengers_asn, df_flights.flight_id == df_flights_passengers_asn.flight_id, "inner")

df_aggregated = df_flights.groupBy("destinationCountry", "dayOfWeek", "season").agg(
        F.count("*").alias("number_of_passengers"))

df_result = df_aggregated.orderBy(F.col("number_of_passengers").desc())


df_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(target_path)

spark.stop()