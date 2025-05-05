from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F
from utils import get_spark_session

layer = "normalized"
table_name = "flights_passengers_asn"

source_file_path = sys.argv[3] + "/staging/flights_passengers_asn/"
target_path = f"{sys.argv[3]}/{layer}/{table_name}"

catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"

spark = get_spark_session(app_name="load_flights_passengers_asn", catalog_name=catalog_name, warehouse_path=sys.argv[3])

df = spark.read.format("parquet").load(source_file_path)

df_target = df.repartition("event_ts").sortWithinPartitions("event_ts")

if not spark.catalog.tableExists(f"{catalog_name}.{layer}.{table_name}"):
        (df_target.writeTo(f"{catalog_name}.{layer}.{table_name}")
        .partitionedBy(F.days("event_ts"))
        .tableProperty("location", target_path)
        .tableProperty("format-version", "2")
        .createOrReplace())
else:
        df_target.createOrReplaceTempView("source")
        merge_sql = f"""
        MERGE INTO {catalog_name}.{layer}.{table_name} AS target
        USING source
        ON target.uci = source.uci AND target.flight_id = source.flight_id AND target.event_ts = source.event_ts AND target.bookingStatus = source.bookingStatus
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        spark.sql(merge_sql)

spark.stop()