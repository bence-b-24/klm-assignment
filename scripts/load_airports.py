from pyspark.sql import SparkSession
import sys
from pyspark.sql import types as T
import os
from py4j.java_gateway import java_import

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

layer = "normalized"
table_name = "airports"

source_file_path = sys.argv[1]
target_path = f"{sys.argv[3]}/{layer}/{table_name}" 
archive_path = f"{sys.argv[4]}/airports/"
catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"


if catalog_name == "hdfs_catalog":
    spark = (SparkSession.builder.appName("load_airports")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.hdfs_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hdfs_catalog.type", "hadoop")
            .config("spark.sql.catalog.hdfs_catalog.warehouse", sys.argv[3])
            .config("spark.sql.defaultCatalog", catalog_name)
            .master("spark://spark-master:7077").getOrCreate())
else:
    spark = (SparkSession.builder.appName("load_airports")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local_catalog.type", "hadoop")
        .config("spark.sql.catalog.local_catalog.warehouse", sys.argv[3])
        .config("spark.sql.defaultCatalog", catalog_name)
        .master("spark://spark-master:7077").getOrCreate())

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