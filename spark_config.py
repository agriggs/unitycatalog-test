from pyspark.sql import SparkSession

def get_spark_session(catalog_url, catalog, token):
    """
    Create a Spark session with Unity Catalog configuration.
    
    :param catalog_url: The URL of the Unity Catalog.
    :param catalog: The name of the catalog.
    :param token: The authentication token for Unity Catalog.
    :return: A configured Spark session.
    """
    
    unity_jars = "io.delta:delta-spark_2.12:3.3.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0"
    azure_storage_jars = "org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-azure:3.3.4"

    # Create a Spark session with Unity Catalog configuration
    spark = SparkSession.builder \
        .appName("local-unity-catalog-test") \
        .master("local[*]") \
        .config("spark.jars.packages", f"{unity_jars},{azure_storage_jars}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
        .config(f"spark.sql.catalog.{catalog}", "io.unitycatalog.spark.UCSingleCatalog") \
        .config(f"spark.sql.catalog.{catalog}.uri", catalog_url) \
        .config(f"spark.sql.catalog.{catalog}.token", token) \
        .config("spark.sql.defaultCatalog", catalog) \
        .getOrCreate()
    
    return spark


# from _env import *

# catalog = oss_unity_catalog
# catalog_url = oss_unity_catalog_url
# token = oss_unity_catalog_admin_token

# spark = get_spark_session(catalog_url, catalog, token)