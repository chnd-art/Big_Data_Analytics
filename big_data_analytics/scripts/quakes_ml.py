import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np 

spark = SparkSession.builder \
        .appName("ml") \
        .master("spark://hadoop-master:7077") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.0") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

"""
Data Pre-processing
"""

# Charger le fichier de données de test dans un dataframe.
df_test = spark.read.csv("hdfs:///user/root/input/database.csv", header=True)

df_train = spark.read.parquet("hdfs:///user/root/Quake/quakes/")

# Sélectionner les champs que nous utiliserons et supprimer les champs dont nous n'avons pas besoin.
df_test_clean = df_test['time', 'latitude', 'longitude', 'Magnitude', 'depth']

# Renommer les champs.
df_test_clean = df_test_clean.withColumnRenamed('time', 'Date')\
    .withColumnRenamed('latitude', 'Latitude')\
    .withColumnRenamed('longitude', 'Longitude')\
    .withColumnRenamed('depth', 'Depth')

# Convertir certains champs de chaîne de caractères en champs numériques.
df_test_clean = df_test_clean.withColumn('Latitude', df_test_clean['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df_test_clean['Longitude'].cast(DoubleType()))\
    .withColumn('Depth', df_test_clean['Depth'].cast(DoubleType()))\
    .withColumn('Magnitude', df_test_clean['Magnitude'].cast(DoubleType()))

# Créer les dataframes d'entraînement et de test.
df_testing = df_test_clean['Latitude', 'Longitude', 'Magnitude', 'Depth']
df_training = df_train['Latitude', 'Longitude', 'Magnitude', 'Depth']

#  Supprimer les enregistrements avec des valeurs nulles de nos dataframes.
df_testing = df_testing.dropna()
df_training = df_training.dropna()

"""
Building the machine learning model
"""

# Sélectionner les caractéristiques à analyser dans notre modèle, puis créer le vecteur de caractéristiques.
assembler = VectorAssembler(inputCols=['Latitude', 'Longitude', 'Depth'], outputCol='features')

# Créer le modèle
model_reg = RandomForestRegressor(featuresCol='features', labelCol='Magnitude')

# Chaîner l'assembleur avec le modèle dans une pipeline.
pipeline = Pipeline(stages=[assembler, model_reg])

# Entraîner le modèle.
model = pipeline.fit(df_training)

# Faire la prédiction.
pred_results = model.transform(df_testing)

# Évaluer le modèle
# rmse doit être inférieur à 0,5 pour que le modèle soit utile
evaluator = RegressionEvaluator(labelCol='Magnitude', predictionCol='prediction', metricName='rmse')
rmse = evaluator.evaluate(pred_results)

"""
Create the prediction dataset
"""

# Créer le dataset de prédiction.
df_pred_results = pred_results['Latitude', 'Longitude', 'prediction']

# Renommer le champ de prédiction.
df_pred_results = df_pred_results.withColumnRenamed('prediction', 'Pred_Magnitude')

# Ajouter plus de colonnes
df_pred_results = df_pred_results.withColumn('Year', lit(2025))\
    .withColumn('RMSE', lit(rmse))

# Charger le dataset de prediction dans hdfs
df_pred_results.write.mode("overwrite").parquet("hdfs:///user/root/Quake/pred_results")

print(df_pred_results.show(5))

print('INFO: Job ran successfully')
print('')