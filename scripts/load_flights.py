from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F
from utils import get_spark_session

layer = "normalized"
table_name = "flights"

source_file_path = sys.argv[3] + "/staging/flights/"
target_path = f"{sys.argv[3]}/{layer}/{table_name}"

catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"

spark = get_spark_session(app_name="load_flights", catalog_name=catalog_name, warehouse_path=sys.argv[3])

df_airports = spark.read.format("iceberg").load(f"{catalog_name}.{layer}.airports").select("IATA", "Country", "Timezone")

df = spark.read.format("parquet").load(source_file_path)

df_target = df.join(df_airports, F.col("originAirport") == F.col("IATA"), "left").drop("IATA").withColumnRenamed("Country", "originCountry").withColumnRenamed("Timezone", "originTimezone")
df_target = df_target.join(df_airports, F.col("destinationAirport") == F.col("IATA"), "left").drop("IATA").withColumnRenamed("Country", "destinationCountry").withColumnRenamed("Timezone", "destinationTimezone")
df_target = df_target.dropDuplicates(["flight_id"])

if not spark.catalog.tableExists(f"{catalog_name}.{layer}.{table_name}"):
        (df_target.writeTo(f"{catalog_name}.{layer}.{table_name}")
        .tableProperty("location", target_path)
        .tableProperty("format-version", "2")
        .createOrReplace())
else:
        df_target.createOrReplaceTempView("source")
        merge_sql = f"""
        MERGE INTO {catalog_name}.{layer}.{table_name} AS target
        USING source
        ON target.flight_id = source.flight_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        spark.sql(merge_sql)

spark.stop()