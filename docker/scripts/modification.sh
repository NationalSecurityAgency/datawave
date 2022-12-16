#!/bin/bash

MODIFICATION_ENDPOINT=https://localhost:9343/modification/v1

MAX_PAGES=100

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
openssl ${params} pkcs12 \
    -in ${P12_KEYSTORE} -passin env:P12_KEYSTORE_PASS \
    -out ${TMP_PEM} -nodes 2>/dev/null
opensslexit=$?
umask $OLD_UMASK
[ $opensslexit = 0 ] || errormsg "Error creating temporary certificate file"

FOLDER="modification_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

SYSTEM_FROM=$(hostname)

echo "$(date): polling modification service for configurations"
curl -s -D headers_1.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${MODIFICATION_ENDPOINT}/listConfigurations -o modificationConfigurationResponse.txt -w '%{http_code}\n'

echo "$(date): reloading modification service fields"
curl -s -D headers_2.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${MODIFICATION_ENDPOINT}/reloadCache -o modificationReloadCacheResponse.txt -w '%{http_code}\n'

echo "$(date): polling modification service fields"
curl -s -D headers_2.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${MODIFICATION_ENDPOINT}/getMutableFieldList -o modificationFieldListResponse.txt -w '%{http_code}\n'

cd ../
