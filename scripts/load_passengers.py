from pyspark.sql import SparkSession
import sys
from utils import get_spark_session


layer = "normalized"
table_name = "passengers"

source_file_path = sys.argv[3] + "/staging/passengers/"
target_path = f"{sys.argv[3]}/{layer}/{table_name}"

catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"

spark = get_spark_session(app_name="load_passengers", catalog_name=catalog_name, warehouse_path=sys.argv[3])

df = spark.read.format("parquet").load(source_file_path)

df_target = df

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
        ON target.uci = source.uci
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        spark.sql(merge_sql)

spark.stop()