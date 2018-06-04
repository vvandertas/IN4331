#!/usr/bin/env bash
set -e
mvn clean package

~/opt/spark-2.3.0-bin-hadoop2.7/bin/spark-submit \
    --class "com.vdtas.Application" \
    --master local[4]  \
    --driver-memory 3g \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
    --conf "spark.network.timeout=220s" \
    target/spark-test-1.0-SNAPSHOT.jar "zips/" 2