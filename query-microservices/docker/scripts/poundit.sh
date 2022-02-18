#!/bin/sh
for ((i=0; i < 100; i++)); do
  ./query.sh ${i}  > query_${i}.log &
done
