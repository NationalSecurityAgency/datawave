#! /usr/bin/env bash

## Before accumulo-env.sh is loaded, these environment variables are set and can be used in this file:

# cmd - Command that is being called such as tserver, manager, etc.
# basedir - Root of Accumulo installation
# bin - Directory containing Accumulo scripts
# conf - Directory containing Accumulo configuration
# lib - Directory containing Accumulo libraries

############################
# Variables that must be set
############################

## Accumulo logs directory. Referenced by logger config.
ACCUMULO_LOG_DIR="${ACCUMULO_LOG_DIR:-${basedir}/logs}"
## Hadoop installation
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
## Hadoop configuration
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/stack}"
## Zookeeper installation
ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/opt/zookeeper}"

##########################
# Build CLASSPATH variable
##########################

## Verify that Hadoop & Zookeeper installation directories exist
if [[ ! -d $ZOOKEEPER_HOME ]]; then
  echo "ZOOKEEPER_HOME=$ZOOKEEPER_HOME is not set to a valid directory in accumulo-env.sh"
  exit 1
fi
if [[ ! -d $HADOOP_HOME ]]; then
  echo "HADOOP_HOME=$HADOOP_HOME is not set to a valid directory in accumulo-env.sh"
  exit 1
fi

ZK_JARS=$(find "$ZOOKEEPER_HOME/lib/" -maxdepth 1 -name '*.jar' -not -name '*slf4j*' -not -name '*log4j*' | paste -sd: -)
# 1. Ensure Accumulo's own libraries are put before an application-supplied classpath. The DataWave ingest distribution
# intentionally exports its full job classpath; prepending it would downgrade libraries required by the Accumulo launcher.
# 2. datawave-stack-hadoop installs Hadoop from RPMs. Those jars live in $HADOOP_HOME/client rather than the tarball-style
# $HADOOP_HOME/share/hadoop/client location in the image's default configuration.
# 3. conf and lib is set by calling script that sources this env file
#shellcheck disable=SC2154
CLASSPATH="${conf}:${lib}/*:${HADOOP_CONF_DIR}:${ZOOKEEPER_HOME}/*:${ZK_JARS}:/usr/lib/hadoop/client/*${CLASSPATH:+:${CLASSPATH}}"
export CLASSPATH

##################################################################
# Build JAVA_OPTS variable. Defaults below work but can be edited.
##################################################################

## JVM options set for all processes. Extra options can be passed in by setting ACCUMULO_JAVA_OPTS to an array of options.
read -r -a accumulo_initial_opts < <(echo "${ACCUMULO_JAVA_OPTS:-}")
JAVA_OPTS=(
  '-XX:OnOutOfMemoryError=kill -9 %p'
  '-XX:-OmitStackTraceInFastThrow'
  '-Djava.net.preferIPv4Stack=true'
  '-Dcom.google.protobuf.use_unsafe_pre22_gencode'
  "-Daccumulo.native.lib.path=${lib}/native"
  "${accumulo_initial_opts[@]}"
)

## JVM options set for individual applications
# cmd is set by calling script that sources this env file
#shellcheck disable=SC2154
case "${ACCUMULO_RESOURCE_GROUP:-default}" in
  default)
    # shellcheck disable=SC2154
    # $cmd is exported in the accumulo script, but not the accumulo-service script
    case "$cmd" in
      manager) JAVA_OPTS=('-Xmx512m' '-Xms512m' "${JAVA_OPTS[@]}") ;;
      monitor) JAVA_OPTS=('-Xmx1g' '-Xms1g' "${JAVA_OPTS[@]}") ;;
      gc) JAVA_OPTS=('-Xmx256m' '-Xms256m' "${JAVA_OPTS[@]}") ;;
      tserver) JAVA_OPTS=('-Xmx1356m' '-Xms1356m' "${JAVA_OPTS[@]}") ;;
      compactor) JAVA_OPTS=('-Xmx256m' '-Xms256m' "${JAVA_OPTS[@]}") ;;
      sserver) JAVA_OPTS=('-Xmx512m' '-Xms512m' "${JAVA_OPTS[@]}") ;;
      *) JAVA_OPTS=('-Xmx256m' '-Xms64m' "${JAVA_OPTS[@]}") ;;
    esac
    ;;
  *)
    echo "ACCUMULO_RESOURCE_GROUP named $ACCUMULO_RESOURCE_GROUP is not configured in accumulo-env.sh"
    exit 1
    ;;
esac

## JVM options set for logging. Review log4j2.properties file to see how they are used.
JAVA_OPTS=("-Daccumulo.log.dir=${ACCUMULO_LOG_DIR}"
  "-Daccumulo.application=${cmd}${ACCUMULO_SERVICE_INSTANCE:-}_$(hostname)"
  "-Daccumulo.metrics.service.instance=${ACCUMULO_SERVICE_INSTANCE:-}"
  "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
  "${JAVA_OPTS[@]}"
)

## Optionally setup OpenTelemetry SDK AutoConfigure
## See https://github.com/open-telemetry/opentelemetry-java/tree/main/sdk-extensions/autoconfigure
#JAVA_OPTS=('-Dotel.traces.exporter=jaeger' '-Dotel.metrics.exporter=none' '-Dotel.logs.exporter=none' "${JAVA_OPTS[@]}")

## Optionally setup OpenTelemetry Java Agent
## See https://github.com/open-telemetry/opentelemetry-java-instrumentation for more options
#JAVA_OPTS=('-javaagent:path/to/opentelemetry-javaagent-all.jar' "${JAVA_OPTS[@]}")

case "${cmd}" in
  monitor | gc | manager | tserver | compactor | sserver)
    JAVA_OPTS=('-Dlog4j.configurationFile=log4j2-service.properties' "${JAVA_OPTS[@]}")
    ;;
  *)
    # let log4j use its default behavior (log4j2.properties, etc.)
    true
    ;;
esac

############################
# Variables set to a default
############################

export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-1}"
## Add Hadoop native libraries to shared library paths given operating system
case "$(uname)" in
  Darwin) export DYLD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${DYLD_LIBRARY_PATH}" ;;
  *) export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH:-}" ;;
esac

###############################################
# Variables that are optional. Uncomment to set
###############################################

## ACCUMULO_JAVA_PREFIX can be used to specify commands to prepend to the "java"
## command when it is executed. This can be declared as either a scalar or an
## array variable. The following use of declare to check if it's already set is
## not strictly necessary, but ensures that if you set it in the calling
## environment, that will override what is set here, rather than some mangled
## merged result. You can set the variable any way you like.
#declare -p 'ACCUMULO_JAVA_PREFIX' &>/dev/null || ACCUMULO_JAVA_PREFIX=''
