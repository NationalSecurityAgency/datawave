#!/bin/bash

# Extract the directory and the program name
# takes care of symlinks
PRG="$0"
while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG="`dirname "$PRG"`/$link"
  fi
done
DIRNAME=`dirname "$PRG"`
PROGNAME=`basename "$PRG"`

cd $DIRNAME

if [ -z $JBOSS_HOME ]; then
    echo -n "JBoss Installation Dir? "
    read JBOSS_HOME
    if [ -z $JBOSS_HOME ]; then
        echo "JBoss Installation Dir is required!"
        exit 1
    fi
fi

if [ -z $USER_DN ]; then
	echo -n "User Name? [$USER] "
	read USER_DN
	if [ -z $USER_DN ]; then
	    USER_DN=$USER
	fi
fi

if [ -z $JBOSS_HOST ]; then
	HOSTNAME=`hostname`
	echo -n "JBoss Host? [$HOSTNAME]: "
	read JBOSS_HOST
	if [ -z $JBOSS_HOST ]; then
		JBOSS_HOST=$HOSTNAME
	fi
fi

if [ -z $JBOSS_PORT]; then
	echo -n "JBoss Port? [1099]: "
	read JBOSS_PORT
	if [ -z $JBOSS_PORT ]; then
		JBOSS_PORT=1099
	fi
fi

if [ -z $KEYSTORE_PASS ]; then
	echo -n "Keystore password? []: "
	read KEYSTORE_PASS
fi

CLASSPATH=$DIRNAME/
CLASSPATH=$JBOSS_HOME/client/jbossall-client.jar:${CLASSPATH}
for f in target/lib/*.jar; do
  CLASSPATH=$f:${CLASSPATH}
done
CLASSPATH=target/client-login.jar:${CLASSPATH}

TRUSTSTORE=<jks truststore>
TRUSTSTORE_PASS=<password>

KEYSTORE=<pkcs12 keystore>
KEYSTORE_TYPE=PKCS12

JVM_OPTS="-Djava.security.auth.login.config=$DIRNAME/auth.conf"
JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStore=$TRUSTSTORE -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASS" 
JVM_OPTS="$JVM_OPTS -Dlog4j.configuration=$DIRNAME/log4j.properties"

java -classpath $CLASSPATH $JVM_OPTS -Djava.security.auth.login.config=auth.conf datawave.webservice.examples.RemoteClientLoginExample $JBOSS_HOST $JBOSS_PORT $KEYSTORE $KEYSTORE_TYPE "$KEYSTORE_PASS" "$USER_DN"
