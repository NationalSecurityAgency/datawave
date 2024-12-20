#!/bin/bash 

function usage() {
    echo "USAGE: ./compile_protos.sh <versionNumber>"

    exit 1
}

if [ -z "${1}" ] ; then
      usage
      return 1
fi

# test to see if we have protoc installed
VERSION=`protoc --version 2>/dev/null | grep "$1" |  wc -l`
if [ "$VERSION" -ne 1 ] ; then
   # Nope: bail
   echo "::protoc is not available or incorrect version. Requires libprotoc $1"
   exit 0
fi

PROTODIRS="client/src/main/protobuf common-util/src/main/protobuf"

HERE=$(cd `dirname $0` && pwd)

for DIR in $PROTODIRS; do
    cd $HERE/$DIR
    for PROTO in `ls -1 *proto`; do 
        echo "Compiling $HERE/$DIR/$PROTO"
        mkdir -p ../java
        protoc --java_out ../java $PROTO;
    done
done;

cd $HERE
