#!/bin/bash

RUNS=${1:-10}
FOLDER="poundit_${RUNS}_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

for ((i=0; i < ${RUNS}; i++)); do
  ../batchLookupContent.sh >> batchLookupContent.log &
  ../batchLookup.sh >> batchLookup.log &
  ../connectionFactory.sh >> connectionFactory.log &
  ../count.sh >> count.log &
  ../discovery.sh >> discovery.log &
  ../edge.sh >> edge.log &
  ../errorCount.sh >> errorCount.log &
  ../errorDiscovery.sh >> errorDiscovery.log &
  ../errorFieldIndexCount.sh >> errorFieldIndexCount.log &
  ../errorQuery.sh >> errorQuery.log &
  ../fieldIndexCount.sh >> fieldIndexCount.log &
  ../lookupContent.sh >> lookupContent.log &
  ../lookup.sh >> lookup.log &
  ../plan.sh >> plan.log &
  ../predict.sh >> predict.log &
  ../query.sh >> query.log &
  ../streamingQuery.sh >> streamingQuery.log &
done
