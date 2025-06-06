#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 --master yarn --archives /app/.venv.tar.gz#.venv query.py  $1