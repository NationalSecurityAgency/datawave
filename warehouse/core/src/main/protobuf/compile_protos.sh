#!/bin/bash

# Other projects use protostuff but, meh
#
# override the default protof compiler and include path by setting either the PROTOC or PROTOC_INCLUDE_PATH
# environment variable.

SCRIPT_DIR=$(dirname "$0")

PROTOC=${PROTOC:=protoc}
PROTOC_INCLUDE_PATH=${PROTOC_INCLUDE_PATH:=/usr/local/include}
PROTOC_VERSION=$(${PROTOC} --version)
INPUT_FILES=$(ls ${SCRIPT_DIR}/*.proto)

for PROTO in ${INPUT_FILES}; do
    echo "Compiling ${PROTO} to java; SCRIPT_DIR=${SCRIPT_DIR} PROTOC=${PROTOC} PROTOC_INCLUDE_PATH=${PROTOC} PROTOC_VERSION=${PROTOC_VERSION}" 1>&2
    protoc --java_out ${SCRIPT_DIR}/../java $PROTO
done



