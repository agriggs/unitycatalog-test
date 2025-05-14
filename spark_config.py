from pyspark.sql import SparkSession

def get_spark_session(catalog_url, catalog, token):
    """
    Create a Spark session with Unity Catalog configuration.
    
    :param catalog_url: The URL of the Unity Catalog.
    :param catalog: The name of the catalog.
    :param token: The authentication token for Unity Catalog.
    :return: A configured Spark session.
    """
    
    # Create a Spark session with Unity Catalog configuration
    spark = SparkSession.builder \
        .appName("local-unity-catalog-test") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
        .config(f"spark.sql.catalog.{catalog}", "io.unitycatalog.spark.UCSingleCatalog") \
        .config(f"spark.sql.catalog.{catalog}.uri", catalog_url) \
        .config(f"spark.sql.catalog.{catalog}.token", token) \
        .config("spark.sql.defaultCatalog", catalog) \
        .getOrCreate()
    
    return spark