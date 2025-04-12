#!/bin/bash

protoc --java_out ../java SegmentV1.proto
protoc --jsonschema_out=../jsonschema --jsonschema_opt=entrypoint_message=Segment SegmentV1.proto