from config import *

from spark_config import get_spark_session
from databricks_cli import get_databricks_token

# Configuration
databricks_profile = databricks_cli_profile
uc_api_path = "api/2.1/unity-catalog"
uc_catalog_name = databricks_catalog
uc_catalog_url = databricks_unity_catalog_url
schema_name = "govcontracting"
table_name = "cancelled_awards"

def main():
    try:
        # Get Databricks token and set up headers
        # Doesn't work so using PAT
        if databricks_unity_catalog_personal_access_token:
            token = databricks_unity_catalog_personal_access_token
        else:
            token = get_databricks_token(databricks_profile)
    
        logger.info(f"Create Spark session")
        spark = get_spark_session(uc_catalog_url, uc_catalog_name, token)

        # Read the Delta table from Unity Catalog
        logger.info(f"Reading Delta table: {uc_catalog_name}.{schema_name}.{table_name}")
        df = spark.read.table(f"{uc_catalog_name}.{schema_name}.{table_name}")

        # Log schema and row count
        logger.info(f"Successfully read Delta table. Schema:")
        df.printSchema()
        logger.info(f"Total rows: {df.count()}")

        # Show sample data
        logger.info("Sample data from the Delta table:")
        df.show(5)    
              
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()