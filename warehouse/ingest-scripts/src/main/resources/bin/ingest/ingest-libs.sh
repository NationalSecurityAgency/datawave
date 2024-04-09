#!/bin/bash

# Get environment
. ../ingest/ingest-env.sh
. ../ingest/findJars.sh

#
# Jars
#
CLASSPATH=${CONF_DIR}
CLASSPATH=${CLASSPATH}:${COMMON_UTIL_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_JEXL_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_CORE_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_CORE_COMMON_UTIL_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_CORE_GEO_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_CORE_JEXL_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_ACCUMULO_EXTENSIONS_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_QUERY_CORE_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_COMMON_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_ACCUMULO_UTILS_JAR}
CLASSPATH=${CLASSPATH}:${INMEMORY_ACCUMULO_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_BASE_REST_RESPONSES_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_COMMON_UTILS_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_COMMON_SSDEEP_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INDEX_STATS_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INGEST_CORE_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INGEST_CONFIG_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INGEST_CSV_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INGEST_JSON_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INGEST_WIKIPEDIA_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INGEST_NYCTLC_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_INGEST_SSDEEP_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_METADATA_UTILS_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_TYPE_UTILS_JAR}
CLASSPATH=${CLASSPATH}:${CURATOR_FRAMEWORK_JAR}
CLASSPATH=${CLASSPATH}:${CURATOR_UTILS_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_LANG_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_LANG3_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_CONFIGURATION_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_COLLECTIONS_JAR}
CLASSPATH=${CLASSPATH}:${AVRO_JAR}
CLASSPATH=${CLASSPATH}:${HTTPCORE_JAR}
CLASSPATH=${CLASSPATH}:${HTTPCLIENT_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_IO_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_POOL_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_JCI_CORE_JAR}
CLASSPATH=${CLASSPATH}:${COMMONS_JCI_FAM_JAR}
CLASSPATH=${CLASSPATH}:${CAFFEINE_JAR}
CLASSPATH=${CLASSPATH}:${GUAVA_JAR}
CLASSPATH=${CLASSPATH}:${PROTOBUF_JAR}
CLASSPATH=${CLASSPATH}:${SLF4J_JAR}
CLASSPATH=${CLASSPATH}:${LOG4J2_API_JAR}
CLASSPATH=${CLASSPATH}:${LOG4J2_CORE_JAR}
CLASSPATH=${CLASSPATH}:${LOG4J2_12_API_JAR}
CLASSPATH=${CLASSPATH}:${LOG4J2_SLF4J_JAR}
CLASSPATH=${CLASSPATH}:${LUCENE_JAR}
CLASSPATH=${CLASSPATH}:${THRIFT_JAR}
CLASSPATH=${CLASSPATH}:${AC_CORE_JAR}
CLASSPATH=${CLASSPATH}:${AC_MAPRED_JAR}
CLASSPATH=${CLASSPATH}:${AC_SERVER_JAR}
CLASSPATH=${CLASSPATH}:${AC_START_JAR}
CLASSPATH=${CLASSPATH}:${VFS_JAR}
CLASSPATH=${CLASSPATH}:${ASM_JAR}
CLASSPATH=${CLASSPATH}:${KRYO_JAR}
CLASSPATH=${CLASSPATH}:${MINLOG_JAR}
CLASSPATH=${CLASSPATH}:${REFLECT_ASM_JAR}
CLASSPATH=${CLASSPATH}:${INFINISPAN_CORE_JAR}
CLASSPATH=${CLASSPATH}:${INFINISPAN_COMMONS_JAR}
CLASSPATH=${CLASSPATH}:${JBOSS_LOGGING_JAR}
CLASSPATH=${CLASSPATH}:${JGROUPS_JAR}
CLASSPATH=${CLASSPATH}:${ZOOKEEPER_JAR}
CLASSPATH=${CLASSPATH}:${OPENCSV_JAR}
CLASSPATH=${CLASSPATH}:${STREAMLIB}
CLASSPATH=${CLASSPATH}:${JCOMMANDER_JAR}
CLASSPATH=${CLASSPATH}:${OPENTELEMETRY_API_JAR}
CLASSPATH=${CLASSPATH}:${OPENTELEMETRY_CONTEXT_JAR}
CLASSPATH=${CLASSPATH}:${MICROMETER_CORE_JAR}
CLASSPATH=${CLASSPATH}:${MICROMETER_COMMONS_JAR}

#for geo hilbert curve processing
CLASSPATH=${CLASSPATH}:${VECMATH_JAR}
CLASSPATH=${CLASSPATH}:${JTS_CORE_JAR}
CLASSPATH=${CLASSPATH}:${GEOWAVE_CORE_INDEX_JAR}
CLASSPATH=${CLASSPATH}:${GEOWAVE_CORE_STORE_JAR}
CLASSPATH=${CLASSPATH}:${GEOWAVE_CORE_GEOTIME_JAR}
CLASSPATH=${CLASSPATH}:${UZAYGEZEN_JAR}
CLASSPATH=${CLASSPATH}:${GT_OPENGIS_JAR}
CLASSPATH=${CLASSPATH}:${GT_API_JAR}
CLASSPATH=${CLASSPATH}:${GT_DATA_JAR}
CLASSPATH=${CLASSPATH}:${GT_EPSG_JAR}
CLASSPATH=${CLASSPATH}:${GT_MAIN_JAR}
CLASSPATH=${CLASSPATH}:${GT_MD_JAR}
CLASSPATH=${CLASSPATH}:${GT_REF_JAR}
#See findJars.sh -- this variable is currently undefined
#CLASSPATH=${CLASSPATH}:${GT_SHAPE_JAR}

CLASSPATH=${CLASSPATH}:${JAXB_IMPL_JAR}

#for json
CLASSPATH=${CLASSPATH}:${JSON_SIMPLE}

#for query
CLASSPATH=${CLASSPATH}:${DATAWAVE_WS_QUERY_JAR}
CLASSPATH=${CLASSPATH}:${DATAWAVE_WS_CLIENT_JAR}
CLASSPATH=${CLASSPATH}:${PROTOSTUFF_CORE_JAR}
CLASSPATH=${CLASSPATH}:${PROTOSTUFF_API_JAR}
CLASSPATH=${CLASSPATH}:${COMMON_JAR}

#required for edge ingest
CLASSPATH=${CLASSPATH}:${EDGE_KEY_VERSION_CACHE_FILE}

CLASSPATH=${CLASSPATH}:${SPRING_CORE_JAR}
CLASSPATH=${CLASSPATH}:${SPRING_CONTEXT_JAR}
CLASSPATH=${CLASSPATH}:${SPRING_CONTEXT_SUPPORT_JAR}
CLASSPATH=${CLASSPATH}:${SPRING_BEAN_JAR}
CLASSPATH=${CLASSPATH}:${SPRING_AOP_JAR}
CLASSPATH=${CLASSPATH}:${SPRING_EXPRESSION_JAR}

if [[ "$ADDITIONAL_INGEST_LIBS" != "" ]]; then
    CLASSPATH=${CLASSPATH}:$ADDITIONAL_INGEST_LIBS
fi


export HADOOP_USER_CLASSPATH_FIRST=true
