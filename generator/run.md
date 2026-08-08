
# generate
python generator/generate.py --rate 5 --fraud-rate 0.02


# spark reciever
docker exec -d jupyter bash -c "nohup spark-submit /home/jovyan/generator/spark_stream.py > /home/jovyan/lakehouse/bronze/spark_submit.out 2>&1 &"

docker exec -d jupyter bash -c "nohup spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /home/jovyan/generator/spark_stream.py > /home/jovyan/lakehouse/bronze/spark_submit.out 2>&1 &"

# progress tracker
docker exec jupyter tail -f /home/jovyan/lakehouse/bronze/stream.log

## kill
docker exec jupyter pgrep -f spark_stream.py
docker exec jupyter kill -9 <pid>


## Test Cases Passed

The following test cases were successfully verified:

1. **Generate Flow**

   * `generate` completed successfully.
   * Stream was received successfully.
   * `stream.logs` was populated as expected.

2. **Generate Job Restart**

   * Restarted the `generate` job while the stream was in progress.
   * Verified that the stream job resumed successfully.

3. **Stream Listener Crash**

   * Simulated a crash of the stream listener job.
   * Verified the system behavior after the listener failure.

4. **Stream Listener Restart**

   * Restarted the stream listener after the crash.
   * Verified that the stream listener recovered and resumed processing successfully.
