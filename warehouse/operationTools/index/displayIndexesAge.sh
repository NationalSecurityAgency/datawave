#!/bin/bash

# This script requires the following parameters:
# 	-i AccumuloName
# 	-z ZooKeeper(s) ip:port
# 	-t Accumulo table name
# 	-u Accumulo user name
#
# The following are optional:
# 	-b dayBuckets (comma separated list of ints.  eg 90,60,30,5  Default is 180, 90,60,30,14,7,2)
# 	-p password   (If not specified the user will be prompted for it at the console)
#       -c columns (comma separated list of column family may be specified or column family:column qualifiers)
# 	-f outputFileName (file name to use for the generated accumulo shell script.  If not specified only a summary log is displayed)
#
# eg ./displayIndexesAge.sh -i accumuloinstancename -z localhost:2181 -t DatawaveMetadata -u username -f /tmp/cmds.txt -c i:ABCD,f -b 90,150,120,180
#
#

java -classpath target/datawave-index-validation-4.2.0-SNAPSHOT-jar-with-dependencies.jar nsa.datawave.index.validation.AccumuloIndexAgeDisplay "$@"
