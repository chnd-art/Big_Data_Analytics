1 - Creer les trois conteneurs avec

        docker-compose up -d 

2 - Copier la base de donnee vers HDFS 
     
        hadoop fs -mkdir -p /user/root
        hdfs dfs -mkdir input
        hdfs dfs -put /shared_volume/database.csv ./input 


3 - Puis Exécuter les scripts avec spark-submit
    
        docker exec -it hadoop-master-new spark-submit --master spark://hadoop-master-new:7077 /shared_volume/scripts/quakes_etl.py
        docker exec -it hadoop-master-new spark-submit --master spark://hadoop-master-new:7077 /shared_volume/scripts/quakes_ml.py
        docker exec -it hadoop-master-new spark-submit --master spark://hadoop-master-new:7077 /shared_volume/scripts/quakes_snowflake.py

