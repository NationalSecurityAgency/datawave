#!/usr/bin/env bash

ACCUMULO_LOG_DIR="${ACCUMULO_LOG_DIR:-${basedir}/logs}"
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/stack}"
ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/opt/zookeeper}"

ZK_JARS=$(find "${ZOOKEEPER_HOME}/lib" -maxdepth 1 -name '*.jar' \
    -not -name '*slf4j*' -not -name '*log4j*' | paste -sd:)
# Put Accumulo's own libraries before an application-supplied classpath. The
# DataWave ingest distribution intentionally exports its full job classpath;
# prepending it would downgrade libraries required by the Accumulo launcher.
CLASSPATH="${conf}:${lib}/*:${HADOOP_CONF_DIR}:${ZOOKEEPER_HOME}/*:${ZK_JARS}:/usr/lib/hadoop/client/*${CLASSPATH:+:${CLASSPATH}}"
export CLASSPATH

read -r -a accumulo_initial_opts < <(echo "${ACCUMULO_JAVA_OPTS:-}")
JAVA_OPTS=(
    '-XX:OnOutOfMemoryError=kill -9 %p'
    '-XX:-OmitStackTraceInFastThrow'
    '-Djava.net.preferIPv4Stack=true'
    '-Dcom.google.protobuf.use_unsafe_pre22_gencode'
    "-Daccumulo.native.lib.path=${lib}/native"
    "${accumulo_initial_opts[@]}"
)

case "${cmd}" in
    manager | master) JAVA_OPTS=('-Xmx512m' '-Xms512m' "${JAVA_OPTS[@]}") ;;
    monitor) JAVA_OPTS=('-Xmx1g' '-Xms1g' "${JAVA_OPTS[@]}") ;;
    gc) JAVA_OPTS=('-Xmx256m' '-Xms256m' "${JAVA_OPTS[@]}") ;;
    tserver) JAVA_OPTS=('-Xmx1536m' '-Xms1536m' "${JAVA_OPTS[@]}") ;;
    *) JAVA_OPTS=('-Xmx256m' '-Xms64m' "${JAVA_OPTS[@]}") ;;
esac

JAVA_OPTS=(
    "-Daccumulo.log.dir=${ACCUMULO_LOG_DIR}"
    "-Daccumulo.application=${cmd}${ACCUMULO_SERVICE_INSTANCE:-}_$(hostname)"
    "-Daccumulo.metrics.service.instance=${ACCUMULO_SERVICE_INSTANCE:-}"
    '-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector'
    "${JAVA_OPTS[@]}"
)

case "${cmd}" in
    monitor | gc | manager | master | tserver)
        JAVA_OPTS=('-Dlog4j.configurationFile=log4j2-service.properties' "${JAVA_OPTS[@]}")
        ;;
esac

export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-1}"
export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH:-}"
