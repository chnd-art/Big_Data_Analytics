import pyspark
from pyspark.sql import SparkSession  
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("etl") \
        .master("spark://hadoop-master:7077") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.0") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

# Charger le dataset 
df_load = spark.read.csv("hdfs:///user/root/input/database.csv", header=True, inferSchema=True)

# Supprimer les champs dont nous n'avons pas besoin de df_load.
lst_dropped_columns = ['Depth Error', 'Time', 'Depth Seismic Stations','Magnitude Error','Magnitude Seismic Stations','Azimuthal Gap',
                       'Horizontal Distance','Horizontal Error',
    'Root Mean Square','Source','Location Source','Magnitude Source','Status']

df_load = df_load.drop(*lst_dropped_columns)

# Créer un champ année et l'ajouter au dataframe.
df_load = df_load.withColumn('Year', year(to_timestamp('Date', 'dd/MM/yyyy')))

# Construire le dataframe de fréquence en utilisant le champ année et les comptages pour chaque année.
df_quake_freq = df_load.groupBy('Year').count().withColumnRenamed('count', 'Counts')

# Convertir certains champs de chaîne de caractères en types numériques.
df_load = df_load.withColumn('Latitude', df_load['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df_load['Longitude'].cast(DoubleType()))\
    .withColumn('Depth', df_load['Depth'].cast(DoubleType()))\
    .withColumn('Magnitude', df_load['Magnitude'].cast(DoubleType()))

# Créer les champs de magnitude moyenne (avg magnitude) et de magnitude maximale (max magnitude) et les ajouter à df_quake_freq.
df_max = df_load.groupBy('Year').max('Magnitude').withColumnRenamed('max(Magnitude)', 'Max_Magnitude')
df_avg = df_load.groupBy('Year').avg('Magnitude').withColumnRenamed('avg(Magnitude)', 'Avg_Magnitude')

# Joindre df_max et df_avg à df_quake_freq.
df_quake_freq = df_quake_freq.join(df_avg, ['Year']).join(df_max, ['Year'])

# Supprimer les valeurs nulles.
df_load.dropna()
df_quake_freq.dropna()

# Ecrire df_load dans HDFS.
df_load = df_load.withColumnRenamed("Magnitude Type", "Magnitude_Type")

df_load.write.mode("overwrite").parquet("hdfs:///user/root/Quake/quakes")

# Ecrire df_quake_freq dans HDFS
df_quake_freq.write.mode("overwrite").parquet("hdfs:///user/root/Quake/quake_freq")

# Afficher dataframes 
print(df_quake_freq.show(5))
print(df_load.show(5))

print('INFO: Job ran successfully')
print('')