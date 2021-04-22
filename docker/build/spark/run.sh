#! bin/bash
if [ "$SPARK_MODE" == "master" ];
then
    /opt/spark/sbin/start-master.sh
elif [ "$SPARK_MODE" == "worker" ]; 
then
    /opt/spark/sbin/start-worker.sh --memory "$SPARK_WORKER_MEMORY" "$SPARK_MASTER_URL"
else
    echo "Please provide a mode"
fi