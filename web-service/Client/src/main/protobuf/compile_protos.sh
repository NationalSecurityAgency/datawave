#!/bin/bash

# We use protoc to compile protobuf classes for test purposes only.  The actual
# generation of protobuf messages is done by the protostuff library.

for PROTO in `ls -1 *proto`; do protoc --java_out ../../main/java $PROTO; done
