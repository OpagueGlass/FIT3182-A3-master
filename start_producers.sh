cleanup() {
    echo ""
    echo "Stopping producers..."

    kill $PID_A 2>/dev/null
    kill $PID_B 2>/dev/null
    kill $PID_C 2>/dev/null

    exit 0
}

trap cleanup SIGINT SIGTERM

sudo docker exec -i fit3182-a3-master-pyspark-1 \
    python /home/student/src/producer_a.py &
PID_A=$!

sudo docker exec -i fit3182-a3-master-pyspark-1 \
    python /home/student/src/producer_b.py &
PID_B=$!

sudo docker exec -i fit3182-a3-master-pyspark-1 \
    python /home/student/src/producer_c.py &
PID_C=$!

echo "Producers started in background"
echo "Press Ctrl+C to stop all producers"

wait