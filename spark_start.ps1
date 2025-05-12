pyspark --name "local-uc-test" `
    --master "local[4]" `
    --packages "io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0" `
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" `
    --conf "spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog" `
    --conf "spark.sql.catalog.unity=io.unitycatalog.spark.UCSingleCatalog" `
    --conf "spark.sql.catalog.unity.uri=http://localhost:8080" `
    --conf "spark.sql.catalog.unity.token=" `
    --conf "spark.sql.defaultCatalog=unity"
