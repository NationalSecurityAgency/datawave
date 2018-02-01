#!/bin/bash

# Run Wildfly
# LAUNCH_JBOSS_IN_BACKGROUND=1 $WILDFLY_HOME/bin/standalone.sh -c standalone-full.xml

#
# First, undeploy the DATAWAVE web service EAR
#
echo "Undeploying DATAWAVE..."
$WILDFLY_HOME/bin/jboss-cli.sh --connect --command="undeploy ${project.build.finalName}-${build.env}.ear"

#
# Remove the datawave configuration
#
echo "Remove DATAWAVE configuration from the Wildfly server..."
$WILDFLY_HOME/bin/jboss-cli.sh --connect --file=./remove-datawave-configuration.cli

# Shutdown
$WILDFLY_HOME/bin/jboss-cli.sh --connect --command=":shutdown"

#
# Remove Hadoop configuration module
#
rm -r $WILDFLY_HOME/modules/org

#
# Remove certificates
#
rm -r $WILDFLY_HOME/standalone/configuration/certificates

#
# TODO:WILDFLY - Remove users
#
#echo "Creating DATAWAVE users in Wildfly..."
## Set up a user for HornetQ
#$WILDFLY_HOME/bin/add-user.sh -a -u '${hornetq.system.username}' -p '${hornetq.system.password}' -g admin,InternalUser
## Set up a management user
#$WILDFLY_HOME/bin/add-user.sh -u dwadmin -p '${accumulo.user.password}'

echo "Done..."