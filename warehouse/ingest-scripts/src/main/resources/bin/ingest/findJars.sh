#!/bin/bash


findJar (){
  ls -1 ../../lib/$1-[0-9]*.jar | sort | tail -1
}
findFirstJar (){
  ls -1 ../../lib/$1-[0-9]*.jar | sort | head -1
}
findAllJars (){
  ls -1 ../../lib/$1-[0-9]*.jar | sort | paste -sd ':' -
}
findWebserviceJar (){
  ls -1 ../../lib/$1[-0-9.]*jar | sort | head -1
}
findProvenanceJar (){
  ls -1 ../../lib/$1-[0-9.]*.*.jar |  grep -v with-dependencies | sort | tail -1
}
findAccumuloJar (){
  ls -1 $WAREHOUSE_ACCUMULO_LIB/$1-[0-9]*.jar | sort | tail -1
}
findZookeeperJar(){
  result=$(ls -1 $ZOOKEEPER_HOME/$1-*.jar 2>/dev/null | head -1)
  [[ -f $result ]] || result=$(ls -1 $ZOOKEEPER_HOME/lib/$1-*.jar | head -1)
  echo $result
}

CONF_DIR=../../config
DATAWAVE_INDEX_STATS_JAR=$(findJar datawave-index-stats)
DATAWAVE_INGEST_CSV_JAR=$(findJar datawave-ingest-csv)
DATAWAVE_INGEST_JSON_JAR=$(findJar datawave-ingest-json)
DATAWAVE_INGEST_WIKIPEDIA_JAR=$(findJar datawave-ingest-wikipedia)
DATAWAVE_INGEST_NYCTLC_JAR=$(findJar datawave-ingest-nyctlc)
DATAWAVE_INGEST_SSDEEP_JAR=$(findJar datawave-ingest-ssdeep)
DATAWAVE_INGEST_CORE_JAR=$(findJar datawave-ingest-core)
DATAWAVE_INGEST_CONFIG_JAR=$(findJar datawave-ingest-configuration)
DATAWAVE_COMMON_JAR=$(findJar datawave-common)
DATAWAVE_COMMON_SSDEEP_JAR=$(findJar datawave-ssdeep-common)
DATAWAVE_ACCUMULO_EXTENSIONS_JAR=$(findJar datawave-accumulo-extensions)
DATAWAVE_METRICS_CORE_JAR=$(findJar datawave-metrics-core)
DATAWAVE_METADATA_UTILS_JAR=$(findJar metadata-utils)
DATAWAVE_TYPE_UTILS_JAR=$(findJar type-utils)
DATAWAVE_CORE_COMMON_UTIL_JAR=$(findJar datawave-core-common-util)
DATAWAVE_WS_COMMON_UTIL_JAR=$(findWebserviceJar datawave-ws-common-util)
DATAWAVE_CORE_COMMON_JAR=$(findJar datawave-core-common)
DATAWAVE_WS_COMMON_JAR=$(findWebserviceJar datawave-ws-common)
DATAWAVE_CORE_CONNECTION_POOL_JAR=$(findJar datawave-core-connection-pool)
INMEMORY_ACCUMULO_JAR=$(findJar datawave-in-memory-accumulo)
DATAWAVE_ACCUMULO_UTILS_JAR=$(findJar accumulo-utils)
DATAWAVE_BASE_REST_RESPONSES_JAR=$(findJar base-rest-responses)
DATAWAVE_COMMON_UTILS_JAR=$(findJar common-utils)
DATAWAVE_CORE_JAR=$(findJar datawave-core)
DATAWAVE_CORE_QUERY_JAR=$(findJar datawave-core-query)
DATAWAVE_WS_QUERY_JAR=$(findWebserviceJar datawave-ws-query)
DATAWAVE_WS_CLIENT_JAR=$(findWebserviceJar datawave-ws-client)
CURATOR_FRAMEWORK_JAR=$(findJar curator-framework)
CURATOR_UTILS_JAR=$(findJar curator-client)
COMMONS_LANG_JAR=$(findJar commons-lang)
COMMONS_LANG3_JAR=$(findJar commons-lang3)
COMMONS_COLLECTIONS_JAR=$(findJar commons-collections4)
COMMONS_CONFIGURATION_JAR=$(findJar commons-configuration)
AVRO_JAR=$(findJar avro)
HTTPCLIENT_JAR=$(findJar httpclient)
HTTPCORE_JAR=$(findJar httpcore)
COMMONS_IO_JAR=$(findJar commons-io)
STREAMLIB=$(findJar stream)
COMMONS_POOL_JAR=$(findJar commons-pool)
COMMONS_JCI_CORE_JAR=$(findJar commons-jci-core)
COMMONS_JCI_FAM_JAR=$(findJar commons-jci-fam)
CAFFEINE_JAR=$(findJar caffeine)
GUAVA_JAR=$(findJar guava)
FAILURE_ACCESS_JAR=$(findJar failureaccess)
PROTOBUF_JAR=$(findJar protobuf-java)
SLF4J_JAR=$(findJar slf4j-api)
LOG4J2_API_JAR=$(findJar log4j-api)
LOG4J2_CORE_JAR=$(findJar log4j-core)
LOG4J2_12_API_JAR=$(findJar log4j-1.2-api)
LOG4J2_SLF4J_JAR=$(findJar log4j-slf4j-impl)
JSON_SIMPLE=$(findJar json-simple)
LUCENE_JAR=$(findJar lucene-core)
LUCENE_JAR=$LUCENE_JAR:$(findJar lucene-queryparser)
LUCENE_JAR=$LUCENE_JAR:$(findJar lucene-analyzers-common)
THRIFT_JAR=$(findJar libthrift)
AC_CORE_JAR=$(findAccumuloJar accumulo-core)
AC_SERVER_JAR=$(findAccumuloJar accumulo-server-base)
AC_START_JAR=$(findAccumuloJar accumulo-start)
AC_MAPRED_JAR=$(findAccumuloJar accumulo-hadoop-mapreduce)
VFS_JAR=`ls -1 $WAREHOUSE_ACCUMULO_LIB/commons-vfs*.jar | sort | head -1`
ASM_JAR=$(findJar asm)
KRYO_JAR=$(findJar kryo)
MINLOG_JAR=$(findJar minlog)
REFLECT_ASM_JAR=$(findJar reflectasm)
INFINISPAN_CORE_JAR=$(findJar infinispan-core)
INFINISPAN_COMMONS_JAR=$(findJar infinispan-commons)
JBOSS_LOGGING_JAR=$(findJar jboss-logging)
JGROUPS_JAR=$(findJar jgroups)
ZOOKEEPER_JAR=$(findZookeeperJar zookeeper)
ZOOKEEPER_JUTE_JAR=$(findZookeeperJar zookeeper-jute)
DATAWAVE_QUERY_CORE_JAR=$(findJar datawave-query-core)
COMMONS_JEXL_JAR=$(findJar commons-jexl3)
PROTOSTUFF_API_JAR=$(findJar protostuff-api)
PROTOSTUFF_CORE_JAR=$(findJar protostuff-core)
JAXRS_API_JAR=$(findJar jaxrs-api)
EDGE_KEY_VERSION_CACHE_FILE=${CONF_DIR}/edge-key-version.txt
OPENCSV_JAR=$(findJar opencsv)
SPRING_CORE_JAR=$(findJar spring-core)
SPRING_CONTEXT_JAR=$(findJar spring-context)
SPRING_CONTEXT_SUPPORT_JAR=$(findJar spring-context-support)
SPRING_BEAN_JAR=$(findJar spring-beans)
SPRING_AOP_JAR=$(findJar spring-aop)
SPRING_EXPRESSION_JAR=$(findJar spring-expression)
COMMON_JAR=$(findJar datawave-ws-common)
JCOMMANDER_JAR=$(findJar jcommander)
OPENTELEMETRY_API_JAR=$(findJar opentelemetry-api)
OPENTELEMETRY_CONTEXT_JAR=$(findJar opentelemetry-context)
MICROMETER_CORE_JAR=$(findJar micrometer-core)
MICROMETER_COMMONS_JAR=$(findJar micrometer-commons)

#for geo hilbert curve processing
JTS_CORE_JAR=$(findJar jts-core)
GEOWAVE_CORE_INDEX_JAR=$(findJar geowave-core-index)
GEOWAVE_CORE_STORE_JAR=$(findJar geowave-core-store)
GEOWAVE_CORE_GEOTIME_JAR=$(findJar geowave-core-geotime)
UZAYGEZEN_JAR=$(findJar uzaygezen-core)
VECMATH_JAR=$(findJar vecmath)
GT_OPENGIS_JAR=$(findJar gt-opengis)
GT_API_JAR=$(findJar gt-api)
GT_EPSG_JAR=$(findJar gt-epsg-wkt)
GT_MAIN_JAR=$(findJar gt-main)
GT_MD_JAR=$(findJar gt-metadata)
GT_REF_JAR=$(findJar gt-referencing)
JAXB_IMPL_JAR=$(findJar resteasy-jaxb-provider)

# extra jars
COMMONS_CLI_JAR=$(findJar commons-cli)
COMMONS_LOGGING_JAR=$(findJar commons-logging)
COMMONS_CODEC_JAR=$(findJar commons-codec)
