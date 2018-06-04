#!/usr/bin/env bash
set -e
mvn clean package

~/opt/spark-2.3.0-bin-hadoop2.7/bin/spark-submit \
    --class "com.vdtas.SqlApplication" \
    --master local[4]  \
    --driver-memory 4g \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j-sql.properties" \
    --conf "spark.network.timeout=220s" \
    target/spark-test-1.0-SNAPSHOT.jar "zips/" "input/csv/" 2 "target/out/sql/"