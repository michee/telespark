#!/bin/bash

##SPARK_MASTER="spark://localhost:7077" \

env \
SPARK_HOME="." \
SPARK_MASTER="local[4]" \
INPUT_PATH="./Archive" \
OUTPUT_PATH="./MYRESULT" \
sbt run