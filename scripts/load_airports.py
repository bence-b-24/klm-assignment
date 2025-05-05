from pyspark.sql import SparkSession
import sys
from pyspark.sql import types as T
import os
from py4j.java_gateway import java_import
from utils import move_local_files, move_hdfs_files, get_spark_session

layer = "normalized"
table_name = "airports"

source_file_path = sys.argv[1]
target_path = f"{sys.argv[3]}/{layer}/{table_name}" 
archive_path = f"{sys.argv[4]}/airports/"
catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"

spark = get_spark_session(app_name="load_airports", catalog_name=catalog_name, warehouse_path=sys.argv[3])

airports_schema = T.StructType([
    T.StructField("Airport ID", T.IntegerType(), True),
    T.StructField("Name", T.StringType(), True),
    T.StructField("City", T.StringType(), True),
    T.StructField("Country", T.StringType(), True),
    T.StructField("IATA", T.StringType(), True),
    T.StructField("ICAO", T.StringType(), True),
    T.StructField("Latitude", T.DoubleType(), True),
    T.StructField("Longitude", T.DoubleType(), True),
    T.StructField("Altitude", T.IntegerType(), True),
    T.StructField("Timezone", T.DoubleType(), True),
    T.StructField("DST", T.StringType(), True),
    T.StructField("TZ", T.StringType(), True),
    T.StructField("Type", T.StringType(), True),
    T.StructField("Source", T.StringType(), True)
])

df_airports = spark.read.format("csv").option("header", "false").schema(airports_schema).load(source_file_path)

    
if df_airports.count() == 0:
    print(f"[INFO] Source file is missing or empty: {source_file_path}")
    spark.stop()
    sys.exit(0)

(
    df_airports.writeTo(f"{catalog_name}.{layer}.{table_name}")
        .tableProperty("location", target_path)
        .tableProperty("format-version", "2")
        .createOrReplace()
)

if catalog_name == "hdfs_catalog":
    move_hdfs_files(spark, source_file_path, archive_path)
else:
    move_local_files(source_file_path, archive_path)

spark.stop()