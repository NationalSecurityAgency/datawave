#!/bin/bash

# Automates much of the process of generating 
# the Certificate Authority, Client and Server Keypairs

set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export NAME=example
export ORG=example

export PW="ChangeIt"

# If you have pwgen installed, do this
#export PW=`pwgen -Bs 10 1`
#echo ${PW} > ${DIR}/password
# and then in other scripts you can do
# export PW=`cat password`

cfssl gencert -initca ca-csr.json | cfssljson -bare $ORG-ca

cfssl gencert \
  -ca=$ORG-ca.pem \
  -ca-key=$ORG-ca-key.pem \
  -config=ca-config.json \
  -profile=server \
   server-csr.json | cfssljson -bare ${NAME}-server

cfssl gencert \
  -ca=$ORG-ca.pem \
  -ca-key=$ORG-ca-key.pem  \
  -config=ca-config.json \
  -profile=client \
   client-csr.json | cfssljson -bare ${NAME}-client

# Create PKCS12 store and JKS stores for Java based systems.
#
# Both formats are broken, so in order to get the correct result
# we have to treat trust stores and keystore differently.
#
# For truststore: create in JKS format, convert to PKCS12
# For keystores, create in PKCS12 format, convert to JKS.

openssl pkcs12 -export \
  -passout env:PW \
  -inkey ${NAME}-server-key.pem \
  -name "$NAME-server" \
  -in ${NAME}-server.pem \
  -chain \
  -CAfile $ORG-ca.pem \
  -out ${NAME}-server-keystore.p12

keytool -importkeystore \
  -srckeystore ${NAME}-server-keystore.p12 \
  -srcstorepass:env PW \
  -alias "$NAME-server" \
  -srckeypass:env PW \
  -srcstoretype pkcs12 \
  -destkeystore ${NAME}-server-keystore.jks \
  -deststoretype jks \
  -deststorepass:env PW

keytool -import \
  -alias $ORG-ca \
  -file  $ORG-ca.pem \
  -keystore ${NAME}-server-truststore.jks \
  -storepass:env PW << EOF
yes
EOF

keytool -importkeystore \
  -srckeystore ${NAME}-server-truststore.jks \
  -srcstorepass:env PW \
  -srcstoretype JKS \
  -destkeystore ${NAME}-server-truststore.p12 \
  -deststoretype PKCS12 \
  -deststorepass:env PW

## Client keystore and trust store

openssl pkcs12 -export \
  -passout env:PW \
  -inkey ${NAME}-client-key.pem \
  -name "$NAME-client" \
  -in ${NAME}-client.pem \
  -chain \
  -CAfile $ORG-ca.pem \
  -out ${NAME}-client-keystore.p12 

keytool -importkeystore \
  -srckeystore ${NAME}-client-keystore.p12 \
  -srcstorepass:env PW \
  -alias "$NAME-client" \
  -srckeypass:env PW \
  -srcstoretype pkcs12 \
  -destkeystore ${NAME}-client-keystore.jks \
  -deststoretype jks \
  -deststorepass:env PW

keytool -import \
  -alias $ORG-ca \
  -file  $ORG-ca.pem \
  -keystore ${NAME}-client-truststore.jks \
  -storepass:env PW << EOF
yes
EOF

# Import CA certs
keytool -importkeystore \
  -srckeystore $JAVA_HOME/jre/lib/security/cacerts \
  -srcstorepass changeit \
  -srcstoretype jks \
  -destkeystore ${NAME}-client-truststore.jks \
  -deststoretype jks \
  -storepass:env PW

keytool -importkeystore \
  -srckeystore ${NAME}-client-truststore.jks \
  -srcstorepass:env PW \
  -srcstoretype jks \
  -destkeystore ${NAME}-client-truststore.p12 \
  -deststoretype pkcs12 \
  -deststorepass:env PW
