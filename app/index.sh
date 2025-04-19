#!/bin/bash

HDFS_TMP_BASE="/tmp/index"
HDFS_PIPE1_OUTPUT="$HDFS_TMP_BASE/pipe1_output"
HDFS_PIPE2_OUTPUT="$HDFS_TMP_BASE/pipe2_output"

MAPPER1="mapreduce/mapper1.py"
REDUCER1="mapreduce/reducer1.py"
MAPPER2="mapreduce/mapper2.py"
REDUCER2="mapreduce/reducer2.py"

echo "Creating Cassandra storage..."
python3 app.py

echo "Cleaning up temporary data..."
hdfs dfs -rm -r -f $HDFS_TMP_BASE


# Pipeline 1
echo "Starting Pipeline 1..."
hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar\
  -files "$MAPPER1","$REDUCER1" \
  -archives .venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py" \
  -input "$1" \
  -output "$HDFS_PIPE1_OUTPUT"

if [ $? -ne 0 ]; then
  echo "Pipeline 1 failed."
  exit 1
fi
echo "Pipeline 1 finished."


# Pipeline 2
echo "Starting Pipeline 2..."
mapred streaming \
  -files "$MAPPER2","$REDUCER2" \
  -archives .venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper2.py" \
  -reducer ".venv/bin/python reducer2.py" \
  -input "$HDFS_PIPE1_OUTPUT" \
  -output "$HDFS_PIPE2_OUTPUT"


if [ $? -ne 0 ]; then
  echo "Pipeline 2 failed."
  exit 1
fi
echo "Pipeline 2 finished."


echo "Cleaning up temporary data..."
hdfs dfs -rm -r -f $HDFS_TMP_BASE

echo "Indexing Complete."
exit 0