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



CONF_DIR=../../config
DATAWAVE_INDEX_STATS_JAR=$(findJar datawave-index-stats)
DATAWAVE_INGEST_CSV_JAR=$(findJar datawave-ingest-csv)
DATAWAVE_INGEST_JSON_JAR=$(findJar datawave-ingest-json)
DATAWAVE_INGEST_WIKIPEDIA_JAR=$(findJar datawave-ingest-wikipedia)
DATAWAVE_INGEST_NYCTLC_JAR=$(findJar datawave-ingest-nyctlc)
DATAWAVE_INGEST_CORE_JAR=$(findJar datawave-ingest-core)
DATAWAVE_INGEST_CONFIG_JAR=$(findJar datawave-ingest-configuration)
DATAWAVE_COMMON_JAR=$(findJar datawave-common)
DATAWAVE_BALANCERS_JAR=$(findJar datawave-balancers)
DATAWAVE_METRICS_CORE_JAR=$(findJar datawave-metrics-core)
DATAWAVE_METADATA_UTILS_JAR=$(findJar metadata-utils)
DATAWAVE_TYPE_UTILS_JAR=$(findJar type-utils)
COMMON_UTIL_JAR=$(findWebserviceJar datawave-ws-common-util)
COMMON_JAR=$(findWebserviceJar datawave-ws-common)
INMEMORY_ACCUMULO_JAR=$(findJar datawave-in-memory-accumulo)
DATAWAVE_ACCUMULO_UTILS_JAR=$(findJar accumulo-utils)
DATAWAVE_BASE_REST_RESPONSES_JAR=$(findJar base-rest-responses)
DATAWAVE_COMMON_UTILS_JAR=$(findJar common-utils)
DATAWAVE_CORE_JAR=$(findJar datawave-core)
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
PROTOBUF_JAR=$(findJar protobuf-java)
SLF4J_JAR=$(findJar slf4j-api)
SLF4J_BRIDGE_JAR=$(findJar slf4j-reload4j)
JSON_SIMPLE=$(findJar json-simple)
LOG4J_JAR=$(findJar reload4j)
LOG4J_EXTRAS_JAR=$(findJar apache-log4j-extras)
LUCENE_JAR=$(findJar lucene-core)
LUCENE_JAR=$LUCENE_JAR:$(findJar lucene-queryparser)
LUCENE_JAR=$LUCENE_JAR:$(findJar lucene-analyzers-common)
THRIFT_JAR=$(findJar libthrift)
AC_CORE_JAR=$WAREHOUSE_ACCUMULO_LIB/accumulo-core.jar
AC_SERVER_JAR=$WAREHOUSE_ACCUMULO_LIB/accumulo-server-base.jar
AC_FATE_JAR=$WAREHOUSE_ACCUMULO_LIB/accumulo-fate.jar
AC_START_JAR=$WAREHOUSE_ACCUMULO_LIB/accumulo-start.jar
AC_TRACE_JAR=$WAREHOUSE_ACCUMULO_LIB/accumulo-trace.jar
AC_HTRACE_JAR=$(findJar htrace-core)
VFS_JAR=`ls -1 $WAREHOUSE_ACCUMULO_LIB/commons-vfs*.jar | sort | head -1`
ASM_JAR=$(findJar asm)
KRYO_JAR=$(findJar kryo)
MINLOG_JAR=$(findJar minlog)
REFLECT_ASM_JAR=$(findJar reflectasm)
INFINISPAN_CORE_JAR=$(findJar infinispan-core)
INFINISPAN_COMMONS_JAR=$(findJar infinispan-commons)
JBOSS_LOGGING_JAR=$(findJar jboss-logging)
JGROUPS_JAR=$(findJar jgroups)
ZOOKEEPER_JAR=$ZOOKEEPER_HOME/zookeeper-$ZOOKEEPER_VERSION.jar
DATAWAVE_QUERY_CORE_JAR=$(findJar datawave-query-core)
COMMONS_JEXL_JAR=$(findJar commons-jexl)
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
#XERCES_JAR=$(findJar org.apache.xerces)
JCOMMANDER_JAR=$(findJar jcommander)

#for geo hilbert curve processing
JTS_CORE_JAR=$(findJar jts-core)
GEOWAVE_CORE_INDEX_JAR=$(findJar geowave-core-index)
GEOWAVE_CORE_STORE_JAR=$(findJar geowave-core-store)
GEOWAVE_CORE_GEOTIME_JAR=$(findJar geowave-core-geotime)
UZAYGEZEN_JAR=$(findJar uzaygezen-core)
VECMATH_JAR=$(findJar vecmath)
GT_OPENGIS_JAR=$(findJar gt-opengis)
GT_API_JAR=$(findJar gt-api)
GT_DATA_JAR=$(findJar gt-data)
GT_EPSG_JAR=$(findJar gt-epsg-wkt)
GT_MAIN_JAR=$(findJar gt-main)
GT_MD_JAR=$(findJar gt-metadata)
GT_REF_JAR=$(findJar gt-referencing)
# Currently, gt-shapefile is not getting packaged and only appears in dependency:tree of
# datawave-ws-deploy-application (even w/geowave profile enabled), so removing for now...
#GT_SHAPE_JAR=$(findJar gt-shapefile)
JAXB_IMPL_JAR=$(findJar resteasy-jaxb-provider)

# extra jars
COMMONS_CLI_JAR=$(findJar commons-cli)
COMMONS_LOGGING_JAR=$(findJar commons-logging)
COMMONS_CODEC_JAR=$(findJar commons-codec)
