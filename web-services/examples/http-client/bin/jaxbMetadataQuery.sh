#!/bin/bash

THIS_SCRIPT=$0
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR/..

CLASSPATH=target/datawave-ws-examples-http-client.jar
for f in lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f
done

if (( $# < 4 )); then
    echo "usage: $0 keystore_path keystore_type truststore_path truststore_type [DEBUG]"
    echo "keystore/truststore type values are JKS or PKCS12"
    exit -1
fi

KEYSTORE=$1
KEYSTORE_TYPE=$2
TRUSTSTORE=$3
TRUSTSTORE_TYPE=$4
shift 4

if [ "$1" == "DEBUG" ]; then
    DEBUG_OPTS="-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog -Dorg.apache.commons.logging.simplelog.showdatetime=true -Dorg.apache.commons.logging.simplelog.log.org.apache.http=DEBUG"
    shift
fi

read -sp "Keystore password: " KEYSTORE_PASSWORD
echo
read -sp "Truststore password: " TRUSTSTORE_PASSWORD
echo
TRUSTSTORE_PASSWORD=${TRUSTSTORE_PASSWORD:-secret}

java -Djavax.net.ssl.keyStore=$KEYSTORE -Djavax.net.ssl.keyStoreType=$KEYSTORE_TYPE -Djavax.net.ssl.keyStorePassword=$KEYSTORE_PASSWORD \
    -Djavax.net.ssl.trustStore=$TRUSTSTORE -Djavax.net.ssl.trustStoreType=$TRUSTSTORE_TYPE -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASSWORD \
    $DEBUG_OPTS -cp $CLASSPATH datawave.webservice.examples.JAXBQueryExample $*
