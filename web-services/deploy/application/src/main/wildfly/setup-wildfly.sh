#!/bin/bash

if [ "x$WILDFLY_HOME" = "x" ]; then
    echo "WILDFLY_HOME must be set."
    exit 1
fi
if [ "x$HADOOP_CONF_DIR" = "x" ]; then
    echo "HADOOP_CONF_DIR must be set."
    exit 1
fi
if [ "x$HADOOP_HOME" = "x" ]; then
    echo "HADOOP_HOME must be set."
    exit 1
fi

DEPLOY_EAR=true
EXTRA_OPTS=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --nodeploy)
            DEPLOY_EAR=false
            ;;
        *)
            EXTRA_OPTS="$EXTRA_OPTS \"$1\""
            ;;
    esac
    shift
done

#
# Check to make sure Wildfly is not currently running.
#
ps -ef | grep -F -- '-D[Standalone]'  | grep -v grep > /dev/null
if [ $? -eq 0 ]; then
    echo "Wildfly is currently running. Please stop it first."
    exit 1
fi

#
# Create users
#
echo "Creating DATAWAVE users in Wildfly..."
# Set up a user for HornetQ
$WILDFLY_HOME/bin/add-user.sh -a -u '${hornetq.system.username}' -p '${hornetq.system.password}' -g admin,InternalUser
# Set up a management user
$WILDFLY_HOME/bin/add-user.sh -u '${jboss.jmx.username}' -p '${jboss.jmx.password}'

#
# Copy overlay onto Wildfly
#
cp -rp overlay/* $WILDFLY_HOME/.

#
# Link Hadoop configuration files into our custom hadoop configuration module
#
ln -s $HADOOP_CONF_DIR/*.{xml,xsl,properties} $WILDFLY_HOME/modules/org/apache/hadoop/common/main/hadoop-conf/.
if [[ `uname` == "Darwin" ]]; then
    OSNAME=macosx
else
    OSNAME=linux
fi
ln -s $HADOOP_HOME/lib/native $WILDFLY_HOME/modules/org/apache/hadoop/common/main/lib/${OSNAME}-`uname -m`

#
# Apply the datawave configuration
#
echo "Applying DATAWAVE configuration to Wildfly server..."
$WILDFLY_HOME/bin/jboss-cli.sh --file=./add-datawave-configuration.cli
CONFIG_STATUS=$?
if [ "$CONFIG_STATUS" -ne 0 ]; then
    echo "Failed to deploy Datawave configuration to Wildfly. Please see $JBOSS_CONSOLE_LOG for details."
    exit 3
fi
rm -r $WILDFLY_HOME/standalone/log

#
# Copy the EAR to the deployment dir so it auto-deploys when Wildfly is started.
#
if [ "$DEPLOY_EAR" == "true" ]; then
    cp ear/${project.build.finalName}-${build.env}.ear $WILDFLY_HOME/standalone/deployments
fi
echo "Done..."
