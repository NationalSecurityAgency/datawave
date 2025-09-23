#!/bin/bash

# override the default protof compiler and include path by setting either the PROTOC or PROTOC_INCLUDE_PATH
# environment variable.

SCRIPT_DIR=$(dirname "$0")

PROTOC=${PROTOC:=protoc}
PROTOC_INCLUDE_PATH=${PROTOC_INCLUDE_PATH:=/usr/local/include}
PROTOC_VERSION=$(${PROTOC} --version)
PROTO=AnnotationV1.proto


echo "Compiling ${PROTO} to java;  SCRIPT_DIR=${SCRIPT_DIR} PROTOC=${PROTOC} PROTOC_INCLUDE_PATH=${PROTOC} PROTOC_VERSION=${PROTOC_VERSION}" 1>&2
${PROTOC} --java_out ${SCRIPT_DIR}/../java ${SCRIPT_DIR}/${PROTO}

echo "Compiling ${PROTO} to json schema;  SCRIPT_DIR=${SCRIPT_DIR} PROTOC=${PROTOC} PROTOC_INCLUDE_PATH=${PROTOC} PROTOC_VERSION=${PROTOC_VERSION}" 1>&2
${PROTOC} --jsonschema_out=${SCRIPT_DIR}/../jsonschema --jsonschema_opt=entrypoint_message=Annotation ${SCRIPT_DIR}/${PROTO}
