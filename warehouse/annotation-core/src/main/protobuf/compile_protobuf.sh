#!/bin/bash

protoc --java_out ../java AnnotationV1.proto
protoc --jsonschema_out=../jsonschema --jsonschema_opt=entrypoint_message=Annotation AnnotationV1.proto
