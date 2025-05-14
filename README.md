# Unity Catalog Demos

## Configure Self Hosted Unity Catalog

Follow instructions here: https://docs.unitycatalog.io/quickstart/

For self hosted (Quickstart) used WSL

Docker instuctions: https://docs.unitycatalog.io/docker_compose/

If on an enterprise network that uses intercept certificates, need to update the Docker containers to trust those certificates. Reach out to author for example updates to the Dockerfiles.

## Configure Databricks Unity Catalog

Follow instructions here: https://learn.microsoft.com/en-us/azure/databricks/external-access/admin

## 

## REST APIs

Used Postman to test but can also use curl. Example curl command for self hosted catalog

Catalogs
```
curl http://localhost:8080/api/2.1/unity-catalog/catalogs
```

Schemas
```
curl http://localhost:8080/api/2.1/unity-catalog/schemas?catalog_name=unity
```

Tables
```
curl http://localhost:8080'/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=default'
```

Table
```
curl http://localhost:8080/api/2.1/unity-catalog/tables/unity.default.numbers
```

API documentation
```
http://localhost:8080/docs/#
```

Note for Databricks need to set the authorization token in the REST request, for testing used Personal Access Token (PAT)

## Spark Client

### Configure Java Keystore 

If on an enterprise network that uses intercept certificates, need to update Java keystore to use Spark.

Example commands for adding the certs, on Windows run from Command Prompt as Admin
```
keytool -import -trustcacerts -cacerts -storepass changeit -alias <root-cert-name> -file <root-cert-file>.pem
```

Verify the certs are in the trusted store
```
keytool -list -v --trustcacerts -cacerts -storepass changeit -alias <root-cert-name>
```

### Spark Notebooks

Before running the notebooks, create `_env.py` file updating the `sample_env.py` items

- [OSS Unity Catalog](unity_catalog_oss_spark_client.ipynb)
- [Databricks Unity Catalog](unity_catalog_databricks_spark_client.ipynb)

Both the notebooks use [spark_config.py](./spark_config.py) to create Spark Session
