import subprocess
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
databricks_profile = "enodo"
workspace_url = "https://adb-3947916102150554.14.azuredatabricks.net"
uc_api_path = "api/2.1/unity-catalog"
uc_catalog_name = "enodo"
schema_name = "govcontracting"
table_name = "awards_details"

def get_databricks_token():
    """Get Databricks OAuth token using CLI"""
    try:
        result = subprocess.run(
            ['databricks', 'auth', 'token', '--profile', databricks_profile, '--output', 'JSON'],
            capture_output=True,
            text=True
        )
        token_data = json.loads(result.stdout)
        if 'access_token' in token_data:
            logger.info("Databricks OAuth token retrieved successfully.")
            return token_data['access_token']
        else:
            raise ValueError("Access token not found in response")
    except Exception as e:
        raise Exception(f"Failed to retrieve Databricks OAuth token: {str(e)}")

def main():
    try:
        # Get Databricks token and set up headers
        token = get_databricks_token()
             
        logger.info(f"Create Spark session")
        spark = SparkSession.builder \
            .appName("UC REST") \
            .master("local[1]") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,com.microsoft.azure:azure-storage:8.6.6,org.apache.hadoop:hadoop-azure:3.3.3") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .getOrCreate()       

        spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        spark.conf.set(f"spark.sql.catalog.{uc_catalog_name}", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        spark.conf.set(f"spark.sql.catalog.{uc_catalog_name}.uri", f"{workspace_url}/{uc_api_path}") 
        spark.conf.set(f"spark.sql.catalog.{uc_catalog_name}.token", token) 
        spark.conf.set("spark.sql.defaultCatalog", uc_catalog_name) 

        # Read the Delta table from Unity Catalog
        logger.info(f"Reading Delta table: {uc_catalog_name}.{schema_name}.{table_name}")
        df = spark.read.table(f"{uc_catalog_name}.{schema_name}.{table_name}")

        # Log schema and row count
        logger.info(f"Successfully read Delta table. Schema:")
        df.printSchema()
        logger.info(f"Total rows: {df.count()}")

        # Show sample data
        # logger.info("Sample data from the Delta table:")
        # df.show(5)


        # path = f"{credentials['storage_url']}"    
        # logger.info(f"Reading data from: {path}")
        # df = spark.read.format("delta").load(path)
                
        # logger.info(f"Successfully read Delta table. Schema:")
        # df.printSchema()
        # logger.info(f"Total rows: {df.count()}")
        
        # Show sample data
        # logger.info("Sample data from the Delta table:")
        # df.show(5)        
              
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()