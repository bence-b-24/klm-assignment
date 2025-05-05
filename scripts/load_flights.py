from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F


layer = "normalized"
table_name = "flights"

source_file_path = sys.argv[3] + "/staging/flights/"
target_path = f"{sys.argv[3]}/{layer}/{table_name}"

catalog_name = "hdfs_catalog" if target_path.startswith("hdfs://") else "local_catalog"

if catalog_name == "hdfs_catalog":
    spark = (SparkSession.builder.appName("load_flights")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.hdfs_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hdfs_catalog.type", "hadoop")
            .config("spark.sql.catalog.hdfs_catalog.warehouse", sys.argv[3])
            .config("spark.sql.defaultCatalog", catalog_name)
            .master("spark://spark-master:7077").getOrCreate())
else:
    spark = (SparkSession.builder.appName("load_flights")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local_catalog.type", "hadoop")
        .config("spark.sql.catalog.local_catalog.warehouse", sys.argv[3])
        .config("spark.sql.defaultCatalog", catalog_name)
        .master("spark://spark-master:7077").getOrCreate())

df_airports = spark.read.format("iceberg").load(f"{catalog_name}.{layer}.airports").select("IATA", "Country", "Timezone")

df = spark.read.format("parquet").load(source_file_path)

df_target = df.join(df_airports, F.col("originAirport") == F.col("IATA"), "left").drop("IATA").withColumnRenamed("Country", "originCountry").withColumnRenamed("Timezone", "originTimezone")
df_target = df_target.join(df_airports, F.col("destinationAirport") == F.col("IATA"), "left").drop("IATA").withColumnRenamed("Country", "destinationCountry").withColumnRenamed("Timezone", "destinationTimezone")
print("before:" + str(df_target.count()))
df_target = df_target.dropDuplicates(["flight_id"])
print("after:" + str(df_target.count()))

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