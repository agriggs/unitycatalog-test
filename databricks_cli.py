import subprocess
import json
import logging

logger = logging.getLogger(__name__)

def get_databricks_token(databricks_profile):
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
            logger.info("Databricks OAuth token retrieved successfully")
            return token_data['access_token']
        else:
            raise ValueError("Access token not found in response")
    except Exception as e:
        raise Exception(f"Failed to retrieve Databricks OAuth token: {str(e)}")