#!/bin/bash

PASSED_TESTS=(${1})
for p in "${PASSED_TESTS[@]}" ; do
    rm -rf "${p%.sh}"_*
done
