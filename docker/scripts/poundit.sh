#!/bin/bash

RUNS=${1:-10}
SCRIPTS=${@:2}
FOLDER="poundit_${RUNS}_$(date +%Y%m%d_%I%M%S.%N)"

if [[ "$SCRIPTS" == "" || "$SCRIPTS" == "all" ]]; then
  SCRIPTS="batchLookupContent.sh batchLookup.sh connectionFactory.sh count.sh discovery.sh edgeEvent.sh edge.sh errorCount.sh errorDiscovery.sh errorFieldIndexCount.sh errorQuery.sh fieldIndexCount.sh hitHighlights.sh lookupContent.sh lookup.sh metrics.sh plan.sh predict.sh query.sh streamingQuery.sh termFrequency.sh"
fi

mkdir $FOLDER
cd $FOLDER

for ((i=0; i < ${RUNS}; i++)); do
  for script in $SCRIPTS; do
    echo "Executing ../${script} >> ${script%%.sh}.log &"
    ../${script} >> ${script%%.sh}.log &
  done
done
