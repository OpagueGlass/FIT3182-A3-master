sudo docker exec -i fit3182-a3-pyspark-1 python /home/student/src/producer_a.py &
sudo docker exec -i fit3182-a3-pyspark-1 python /home/student/src/producer_b.py &
sudo docker exec -i fit3182-a3-pyspark-1 python /home/student/src/producer_c.py &
echo "Producers started in background"
wait
echo "All producers have finished"