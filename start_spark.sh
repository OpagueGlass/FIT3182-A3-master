sudo docker exec -it fit3182-a3-master-pyspark-1 bash -c '$SPARK_HOME/sbin/start-master.sh'

sudo docker exec -it fit3182-a3-master-pyspark-1 python /home/student/src/setup_mongodb.py

sudo docker exec -it fit3182-a3-master-pyspark-1 python /home/student/src/streaming_app.py