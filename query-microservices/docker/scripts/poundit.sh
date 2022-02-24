#!/bin/bash

RUNS=${1:-100}
FOLDER="poundit_${RUNS}_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

for ((i=0; i < ${RUNS}; i++)); do
  ../query.sh >> poundit.log &
done
