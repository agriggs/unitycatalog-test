import subprocess
import json
import logging

from _env import *
from spark_config import get_spark_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
databricks_profile = databricks_cli_profile
workspace_url = "https://adb-3947916102150554.14.azuredatabricks.net"
uc_api_path = "api/2.1/unity-catalog"
uc_catalog_name = databricks_catalog
uc_catalog_url = databricks_unity_catalog_url
schema_name = "govcontracting"
table_name = "cancelled_awards"

def get_databricks_token():
    logger.info(f"Get Databricks OAuth token using CLI for profile: {databricks_profile}")
    try:
        result = subprocess.run(
            ['databricks', 'auth', 'token', '--profile', databricks_profile, '--output', 'JSON'],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"Error retrieving Databricks OAuth token: {result.stderr}")
       
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
        # Doesn't work so using PAT
        token = get_databricks_token()
        # token = databricks_unity_catalog_admin_token

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