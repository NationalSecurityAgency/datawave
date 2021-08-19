#!/bin/bash

DATAWAVE_ENDPOINT=https://localhost:8443/query/v1
METRICS_ENDPOINT=https://localhost:8543/querymetric/v1

MAX_PAGES=100

# use the test user pkcs12 cert
P12_KEYSTORE=../pki/testUser.p12
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

# Create one-time passphrase and certificate
OLD_UMASK=$(umask)
umask 0277
export P12_KEYSTORE_PASS
openssl pkcs12 \
    -in ${P12_KEYSTORE} -passin env:P12_KEYSTORE_PASS \
    -out ${TMP_PEM} -nodes
opensslexit=$?
umask $OLD_UMASK
[ $opensslexit = 0 ] || errormsg "Error creating temporary certificate file"

read_dom () {
    local IFS=\>
    read -d \< ENTITY CONTENT
}

get_query_id () {
    while read_dom; do
        if [[ $ENTITY =~ 'Result' ]] && [[ ! $ENTITY =~ 'HasResults'  ]]; then
            echo $CONTENT
            break
        fi
    done
}

get_num_events () {
    while read_dom; do
        if [[ $ENTITY = 'ReturnedEvents' ]]; then
            echo $CONTENT
            break
        fi
    done
}

FOLDER="query_$(date +%Y%m%d_%I%M%S.%3N)"

mkdir $FOLDER
cd $FOLDER

SYSTEM_FROM=$(hostname)

echo "$(date): Creating query" > querySummary.txt
curl -D headers_0.txt -k -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    --data-urlencode "begin=20200101 000000.000" \
    --data-urlencode "end=20210101 000000.000" \
    --data-urlencode "columnVisibility=PUBLIC" \
    --data-urlencode "query=FIELD:VALUE" \
    --data-urlencode "query.syntax=LUCENE" \
    --data-urlencode "auths=PUBLIC" \
    --data-urlencode "systemFrom=$SYSTEM_FROM" \
    --data-urlencode "queryName=Developer Test Query" \
    --data-urlencode "pageSize=100" \
    ${DATAWAVE_ENDPOINT}/EventQuery/create -o createResponse.txt

i=1

QUERY_ID=$(get_query_id < createResponse.txt)

while [ $i -gt 0 ] && [ $i -lt $MAX_PAGES ]; do
    echo "$(date): Requesting page $i for $QUERY_ID" >> querySummary.txt
    curl -D headers_$i.txt -q -k -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        ${DATAWAVE_ENDPOINT}/$QUERY_ID/next -o nextResponse_$i.txt

    CONTINUE=`grep 'HTTP/1.1 200 OK' headers_$i.txt`

    if [ -z "$CONTINUE" ]; then
        i=-1
    else
        ((i++))
    fi
done

# close the query
curl -q -k -X POST -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    ${DATAWAVE_ENDPOINT}/$QUERY_ID/close -o closeResponse.txt

# grab the metrics
curl -k -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    ${METRICS_ENDPOINT}/id/$QUERY_ID > queryMetrics.html

cd ../

if [ ! -z "$QUERY_ID" ]; then
    mv $FOLDER query_$QUERY_ID
fi
