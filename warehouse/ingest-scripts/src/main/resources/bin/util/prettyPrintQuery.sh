#!/bin/bash

DW_QUICKSTART_DIR="$HADOOP_HOME/.."

cd $DW_QUICKSTART_DIR/bin/services/datawave/datawave-ingest-install/bin/ingest

findJacksonJars () {
    ls -1 ../../lib/jackson-*.jar | sort | paste -sd ':' -
}

. ./ingest-libs.sh

JACKSON_JARS=$(findJacksonJars)

java -cp $CLASSPATH:$JACKSON_JARS datawave.query.jexl.visitors.JexlStringBuildingVisitor "$@"
