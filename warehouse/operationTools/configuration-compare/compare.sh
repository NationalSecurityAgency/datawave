#!/bin/bash
#
# Shell script to wrap the rather long maven command needed to run this thing.
#
mvn exec:java -Dexec.mainClass=nsa.datawave.configuration.RunCompare -Dexec.args="$1 $2"
