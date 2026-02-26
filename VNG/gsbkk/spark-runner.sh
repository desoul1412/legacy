#!/bin/bash
set -e

AIRFLOW_HOME=/opt/airflow
HDFS_BASE="hdfs://c0s/user/gsbkk-workspace-yc9t6"
HDFS_ARCHIVES="$HDFS_BASE/archives"
PYTHON_ENV_TARBALL="$HDFS_ARCHIVES/environment.tar.gz"

game_id=$1
event_date=$2
models=$3
config_dir="hdfs:///user/gsbkk-workspace-yc9t6/configs"

JARS_OPT=""

if [ $# -gt 3 ] && [ -n "$4" ]; then
  JARS_OPT="--jars $4"
fi

# 3. Run Spark with Python environment
spark-submit \
  --name "gsbkk::$game_id::$event_date::$models" \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 5g \
  --driver-cores 2 \
  --executor-memory 4g \
  --num-executors 5 \
  --executor-cores 4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
  --archives $PYTHON_ENV_TARBALL#environment \
  $JARS_OPT \
  $AIRFLOW_HOME/dags/repo/src/spark_processor.py \
  game_id=$game_id event_date=$event_date models=$models config_dir=$config_dir