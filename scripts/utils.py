

from pyspark.sql import SparkSession
import os
from py4j.java_gateway import java_import

def get_spark_session(app_name: str, catalog_name: str, warehouse_path: str, spark_master: str = "spark://spark-master:7077") -> SparkSession:
    """
    Create a Spark session with the specified catalog name and warehouse path.
    
    Args:
        catalog_name (str): The name of the catalog to use.
        warehouse_path (str): The path to the warehouse directory.
    
    Returns:
        SparkSession: A configured Spark session.
    """
    if catalog_name == "hdfs_catalog":
        return (SparkSession.builder.appName(app_name)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.catalog.hdfs_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hdfs_catalog.type", "hadoop")
                .config("spark.sql.catalog.hdfs_catalog.warehouse", warehouse_path)
                .config("spark.sql.defaultCatalog", catalog_name)
                .master(spark_master).getOrCreate())
    else:
        return (SparkSession.builder.appName(app_name)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local_catalog.type", "hadoop")
                .config("spark.sql.catalog.local_catalog.warehouse", warehouse_path)
                .config("spark.sql.defaultCatalog", catalog_name)
                .master(spark_master).getOrCreate())

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