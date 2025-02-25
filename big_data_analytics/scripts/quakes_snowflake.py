spark = SparkSession.builder \
        .appName("snowflake") \
        .master("spark://hadoop-master:7077") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.0") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

sfOptions = {
    "sfURL" : "https://vv85366.switzerland-north.azure.snowflakecomputing.com",
    "sfDatabase" : "Quake",
    "sfSchema" : "PUBLIC",
    "sfWarehouse" : "COMPUTE_WH",
    "sfRole" : "ACCOUNTADMIN",  
    "sfUser" : "ADMIN",
    "sfPassword" : "BGKUzh72xKF4aCA"
}

# Charger la table Quake depuis HDFS
quake_df_1 = spark.read.parquet("hdfs:///user/root/Quake/quakes")
quake_df_2 = spark.read.parquet("hdfs:///user/root/Quake/quake_freq")
quake_df_3 = spark.read.parquet("hdfs:///user/root/Quake/pred_results")

# Transférer la première table a Snowflake 
quake_df_1.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "quakes") \
    .mode("overwrite") \
    .save()

# Transférer la deuxième table a Snowflake
quake_df_2.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "quake_freq") \
    .mode("overwrite") \
    .save()

# Transférer la troisième table a snowflake
quake_df_3.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "pred_results") \
    .mode("overwrite") \
    .save()