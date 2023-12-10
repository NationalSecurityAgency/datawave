#!/bin/bash

DATAWAVE_ENDPOINT=https://localhost:8443/query/v1/query
METRICS_ENDPOINT=https://localhost:8543/querymetric/v1

POOL="${POOL:-pool1}"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# use the test user pkcs12 cert
P12_KEYSTORE=${SCRIPT_DIR}/../pki/testUser.p12
P12_KEYSTORE_PASS=ChangeIt

TMP_DIR=/dev/shm
TMP_PEM="$TMP_DIR/testUser-$$-pem"

sh -c "while kill -0 $$ 2>/dev/null; do sleep 1; done; rm -f '${TMP_P12}' '${TMP_PEM}'" &

function needsPassphrase() {
    [ -z "${P12_KEYSTORE_PASS}" ]
}

function getFromCliPrompt() {
    read -s -p "Passphrase for ${P12_KEYSTORE}: " P12_KEYSTORE_PASS && echo 1>&2
}

needsPassphrase && getFromCliPrompt

params=""
opensslVersion3="$( openssl version | awk '{ print $2 }' | grep -E ^3\. )"
if [ ! -z "$opensslVersion3" ]; then
    params="-provider legacy -provider default"
fi

# Create one-time passphrase and certificate
OLD_UMASK=$(umask)
umask 0277
export P12_KEYSTORE_PASS
openssl pkcs12 ${params} \
    -in ${P12_KEYSTORE} -passin env:P12_KEYSTORE_PASS \
    -out ${TMP_PEM} -nodes 2>/dev/null
opensslexit=$?
umask $OLD_UMASK
[ $opensslexit = 0 ] || echo "Error creating temporary certificate file"

read_dom () {
    local IFS=\>
    read -d \< ENTITY CONTENT
}

get_query_prediction () {
    while read_dom; do
        if [[ $ENTITY =~ 'Result' ]] && [[ ! $ENTITY =~ 'HasResults'  ]]; then
            echo $CONTENT
            break
        fi
    done
}

FOLDER="prediction_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

SYSTEM_FROM=$(hostname)

echo "$(date): Predicting query"
echo "$(date): Predicting query" > querySummary.txt
curl -s -D headers_0.txt -k -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    -H "Pool: $POOL" \
    --data-urlencode "begin=19660908 000000.000" \
    --data-urlencode "end=20161002 235959.999" \
    --data-urlencode "columnVisibility=PUBLIC" \
    --data-urlencode "query=GENRES:[Action to Western]" \
    --data-urlencode "query.syntax=LUCENE" \
    --data-urlencode "auths=PUBLIC,PRIVATE,BAR,FOO" \
    --data-urlencode "systemFrom=$SYSTEM_FROM" \
    --data-urlencode "queryName=Developer Test Query" \
    ${DATAWAVE_ENDPOINT}/EventQuery/predict -o predictResponse.txt -w '%{http_code}\n' >> querySummary.txt

QUERY_PREDICTION=$(get_query_prediction < predictResponse.txt)

echo "$(date): Received query prediction"
echo "$(date): Received query prediction" >> querySummary.txt

echo "$QUERY_PREDICTION"
echo "$QUERY_PREDICTION" >> querySummary.txt
