sudo docker compose up -d

sudo docker exec -it fit3182-a3-master-pyspark-1 bash -c '$SPARK_HOME/sbin/start-master.sh'

sudo rm -rf /mnt/shared