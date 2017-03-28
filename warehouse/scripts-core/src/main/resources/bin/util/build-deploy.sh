#!/bin/bash

print_usage() {
   echo ""
   echo "Usage:"
   echo ""
   echo " ./build-deploy <sourcehome-required> <deployhome-required> <extra-mavenargs-optional>"
   echo ""
   echo "E.g.,  ./build-deploy ~/datawave ~/my-datawave-instance"
   echo "       ./build-deploy ~/datawave ~/my-datawave-instance '-DskipTests -DskipITs'"
   echo ""
}

#*************************************************************************************
#*************************************************************************************
#
# This script is intended to simplify the process of building and deploying the 
# DataWave 'dev' build on your local development host, so that deployment and testing 
# can be performed in a repeatable and reliable way.
#
# All cluster services, such as DW Ingest, DW Web Service, Accumulo, Hadoop, and ZooKeeper,
# are started/stopped as needed, and as directed by user input in some cases.
#
# For example, the script will prompt at the end to ask whether or not you want to start 
# DW Ingest. Before ingest is started, you'll have the option to reset Accumulo (ie, 
# to delete DW table data, to create new shard table splits, etc). You'll also have the 
# option to push your own test data into configured poller and hdfs input directories 
# for ingestion. Also, Accumulo VFS classpaths can be refreshed in HDFS, if
# desired.
#
# Prerequisites: 
#
#   * Accumulo, Hadoop, and Zookeeper must be installed and initialized, but are not 
#     required to be running before the script is executed
#
#   * If the CachedResults API is to be used, then MySQL should be installed and 
#     initialized properly for DW, and should be running prior to running this script.
#     No attempts are made to manage or interact with MySQL in any way here.
#
#   * DataWave source code must be accessible on the local file system
#
#   * Wildfly will be deleted and reinstalled on each run. Further, for simplicity, the 
#     installation is assumed to be $DEPLOY_HOME/wildfly-9.0.1.Final/ and the file
#     $DEPLOY_HOME/wildfly-9.0.1.Final.tar.gz must already exist.
#
# More notes/assumptions: 
#
#   * This script can be a time saver, but it only attempts to be "smart" to a certain 
#     extent. You must take care to customize environment settings below, if needed, 
#     and possibly customize logic within this script and others, if something doesn't 
#     suit your needs. And opportunities for improvement surely abound, so please help 
#     yourself
#
#   * For simplicity, *all* services are assumed to be owned by and executed as the
#     current user. If you need/want services to execute under distinct user accounts, 
#     then you'll definitely need to modify this script to make that work.
#
#   * DW WebService and Ingest *.tar.gz's will be installed/extracted in $DEPLOY_HOME/,
#     and then automatically symlinked in that directory using the INGEST_SYMLINK and 
#     WEBSERVICE_SYMLINK names defined below.
#
#*************************************************************************************
#*************************************************************************************

SOURCE_HOME=$1
DEPLOY_HOME=$2

[ -z "$SOURCE_HOME" ] && print_usage && exit 1
[ -z "$DEPLOY_HOME" ] && print_usage && exit 1

[ ! -d $SOURCE_HOME ] && echo "$SOURCE_HOME doesn't exist" && print_usage && exit 1
[ ! -d $DEPLOY_HOME ] && echo "$DEPLOY_HOME doesn't exist" && print_usage && exit 1

# Make sure to use quotes for passing multiple args. And see the 'buildAndExitOnFailure'
# function below to view/edit the default maven command that is used
XTRA_MAVEN_OPTS=$3

####################################################################################
########################## ENVIRONMENT SETUP #######################################

[ -z "$HADOOP_HOME" ] && echo "Aborting! Please make sure HADOOP_HOME is set in your environment" && exit 1
[ -z "$ACCUMULO_HOME" ] && echo "Aborting! Please make sure ACCUMULO_HOME is set in your environment" && exit 1
[ -z "$ZOOKEEPER_HOME" ] && echo "Aborting! Please make sure ZOOKEEPER_HOME is set in your environment" && exit 1
[ -z "$WILDFLY_HOME" ] && echo "Aborting! Please make sure WILDFLY_HOME is set in your environment" && exit 1

# Some have observed that their hadoop native libs ($HADOOP_HOME/lib/native/) *may* cause certain
# datawave-poller unit tests to fail. More time needs to be invested to determine the actual 
# problem and solution, but other than failed tests, no further impacts have been observed.
# Meanwhile, there's a workaround here to temporarily 'mv' the libs over to $HADOOP_NATIVE_BACKUP
# prior to running the build, and then copy them back once complete. Kinda lame, but it does avoid 
# the issue entirely. Set NATIVE_BACKUP_ENABLED to true to enable this feature. HADOOP_NATIVE_BACKUP 
# and HADOOP_NATIVE are verified prior to any 'mv' attempt

NATIVE_BACKUP_ENABLED=true
HADOOP_NATIVE=$HADOOP_HOME/lib/native
HADOOP_NATIVE_BACKUP=$DEPLOY_HOME/native_libs_that_break_the_build

INGEST_SYMLINK=datawave-ingest
WEBSERVICE_SYMLINK=datawave-webservice

DW_INGEST_LOG_DIR=$DEPLOY_HOME/logs/$INGEST_SYMLINK

# Verify Wildfly assembly exists
[ ! -f $DEPLOY_HOME/wildfly-9.0.1.Final.tar.gz ] && echo "$DEPLOY_HOME/wildfly-9.0.1.Final.tar.gz not found. Aborting!" && exit 1

# Check to see if the reset-accumulo.sh companion script exists
ACCUMULO_RESET_SCRIPT=$DEPLOY_HOME/reset-accumulo.sh
[ ! -f $ACCUMULO_RESET_SCRIPT ] && echo "$ACCUMULO_RESET_SCRIPT not found. 'Reset' functionality will be disabled"

# Change these ingest test inputs as needed....
# No harm to leave them as-is, if you don't care to test ingest,
# since you'll be prompted to activate testing at runtime.

POLLER_INPUT_FILES="/path/to/input/datatype1/test.dt1 /path/to/input/datatype2/test.dt2"
# Convert to array. Note: POLLER_INPUT_FILES[n] must align with POLLER_INPUT_DIRS[n]
POLLER_INPUT_FILES=( $POLLER_INPUT_FILES )

POLLER_INPUT_DIRS="/poller/dir1 /poller/dir2"
# Convert to array
POLLER_INPUT_DIRS=( $POLLER_INPUT_DIRS )

HDFS_INPUT_FILES="/local/path/to/input/datatype1/test.dt1 /local/path/to/input/datatype2/test.dt2"
# Convert to array. Note: HDFS_INPUT_FILES[n] must align with HDFS_INPUT_DIRS[n]
HDFS_INPUT_FILES=( $HDFS_INPUT_FILES )

HDFS_INPUT_DIRS="/hdfs/input/dir1 /hdfs/input/dir2"
# Convert to array
HDFS_INPUT_DIRS=( $HDFS_INPUT_DIRS )

########################################################################################
########################## STOP SERVICES ###############################################

# Assuming we'll always want to kill Webservice at minimum
# Wildfly nice kill
WILDFLY_PID=$(/usr/bin/jps -lm | grep wildfly-current | cut -d' ' -f1)
[ ! -z "$WILDFLY_PID" ] && kill -15 $WILDFLY_PID && sleep 3
# If Wildfly still running, then bring down hard
WILDFLY_PID=$(/usr/bin/jps -lm | grep wildfly-current | cut -d' ' -f1)
[ ! -z "$WILDFLY_PID" ] && kill -9 $WILDFLY_PID

echo ""
echo "Wildfly stopped"

USER_RESPONSE="n"
INGEST_PID=$(/usr/bin/jps -lm | grep 'nsa.datawave.util.flag.FlagMaker' | grep 'config/FiveMinFlagMaker.xml' | cut -d' ' -f1)
[ ! -z "$INGEST_PID" ] && echo "" && read -r -p "Bring down ingest services? [y/n]: " USER_RESPONSE
[ "$USER_RESPONSE" == "y" ] && cd $DEPLOY_HOME/$INGEST_SYMLINK/bin/system && ./stop-all.sh

echo ""
read -r -p "Bounce Accumulo/Hadoop/ZooKeeper? (maybe not a bad idea if you need to refresh Accumulo VFS, etc) [y/n]: " USER_RESPONSE
if [[ "$USER_RESPONSE" == "y" ]]; then
    HADOOP_PID=$(/usr/bin/jps -lm | grep namenode\.NameNode | cut -d' ' -f1)
    ACCUMULO_PID=$(/usr/bin/jps -lm | grep 'org.apache.accumulo.start.Main master' | cut -d' ' -f1)
    ZK_PID=$(/usr/bin/jps -lm | grep QuorumPeerMain | cut -d' ' -f1)
    INGEST_PID=$(/usr/bin/jps -lm | grep 'nsa.datawave.util.flag.FlagMaker' | grep 'config/FiveMinFlagMaker.xml' | cut -d' ' -f1)
    [ ! -z "$INGEST_PID" ] && echo "Bringing down ingest services" && cd $DEPLOY_HOME/datawave-ingest/bin/system && ./stop-all.sh
    [ ! -z "$ACCUMULO_PID" ] && cd $ACCUMULO_HOME/bin && ./stop-all.sh
    [ ! -z "$ZK_PID" ] && cd $ZOOKEEPER_HOME/bin && ./zkServer.sh stop
    [ ! -z "$HADOOP_PID" ] && cd $HADOOP_HOME/sbin && ./mr-jobhistory-daemon.sh stop historyserver && ./stop-all.sh
fi

########################################################################################
########################## BUILD DATAWAVE  #############################################

#***************************************************************************************
#
# This method will verify native lib backup variables, and
# disable the backup attempt if any dirs are invalid
#
#***************************************************************************************
verifyNativeBackupConfig() {

    if [ "$NATIVE_BACKUP_ENABLED" = true ]; then
       echo ""
       echo "Hadoop native library backup enabled"
       if [ ! -d $HADOOP_NATIVE_BACKUP ]; then
          NATIVE_BACKUP_ENABLED=false
          echo "$HADOOP_NATIVE_BACKUP doesn't exist!"
       fi
       if [ ! -d $HADOOP_NATIVE ]; then
          NATIVE_BACKUP_ENABLED=false
          echo "$HADOOP_NATIVE doesn't exist!"
       fi
    fi
    
    if [ "$NATIVE_BACKUP_ENABLED" != true ]; then 
       echo "Hadoop native library backup disabled"
    fi
}

#***************************************************************************************
#
# This method will execute a full build on $SOURCE_HOME,
# redirecting mvn output to $PROGRESS_FILE. If the build fails,
# we sound the alarm and abort the script
#
#***************************************************************************************
buildAndExitOnFailure() {

    PROGRESS_FILE=$DEPLOY_HOME/temp-build-progress.txt
    [ -f $PROGRESS_FILE ] && rm -f $PROGRESS_FILE

    verifyNativeBackupConfig
    if [ "$NATIVE_BACKUP_ENABLED" = true ]; then
        mv $HADOOP_NATIVE/* $HADOOP_NATIVE_BACKUP
        echo "Backed up native libraries."
        echo ""
    fi

    echo ""
    echo "Build in progress... Inspect the following log to check status: $PROGRESS_FILE"
    echo ""

    cd $SOURCE_HOME
    mvn -Pdev -Ddeploy -Dtar -Ddist clean install $XTRA_MAVEN_OPTS > $PROGRESS_FILE

    if [ "$NATIVE_BACKUP_ENABLED" = true ]; then
        cp $HADOOP_NATIVE_BACKUP/* $HADOOP_NATIVE/
        echo "Restored native backup."
    fi

    echo ""
    tail $PROGRESS_FILE

    BUILD_SUCCESS=$(tail -n 7 $PROGRESS_FILE | grep "BUILD SUCCESS")
    if [ -z "$BUILD_SUCCESS" ]; then
       echo ""
       echo "Exiting. The build has FAILED! You broke it. Now fix it!!!!"
       exit 1
    fi
}

# Pre-check for assemblies (maybe the user doesn't need or want to rebuild, only deploy)
WEBSERVICE_TAR=$(find $SOURCE_HOME -path "*/web-service-ear/target/datawave-web-service-*-dev.tar.gz" -type f)
INGEST_TAR=$(find $SOURCE_HOME -path "*/assemble-core/deploy/target/datawave-dev-*-dist.tar.gz" -type f)

if [ ! -z "$WEBSERVICE_TAR" ] && [ ! -z "$INGEST_TAR" ]; then
   echo ""
   echo "Found web service assembly: $WEBSERVICE_TAR"
   echo "Found ingest assembly: $INGEST_TAR"
   echo ""
   read -r -p "DataWave deployment assemblies already exist. Continue with Maven build? [y/n]: " USER_RESPONSE
   [ "$USER_RESPONSE" == "y" ] && buildAndExitOnFailure
else
   buildAndExitOnFailure
fi

# Post-check for assemblies
WEBSERVICE_TAR=$(find $SOURCE_HOME -path "*/web-service-ear/target/datawave-web-service-*-dev.tar.gz" -type f)
INGEST_TAR=$(find $SOURCE_HOME -path "*/assemble-core/deploy/target/datawave-dev-*-dist.tar.gz" -type f)

[ ! -f $WEBSERVICE_TAR ] && echo "Something went terribly wrong. Web Service assembly doesn't exist!" && exit 1
[ ! -f $INGEST_TAR ] && echo "Something went terribly wrong. Ingest assembly doesn't exist!" && exit 1

DW_VERSION=$(ls -1 $INGEST_TAR | sed "s/.*\///" | sed "s/datawave-dev-//" | sed "s/-dist.tar.gz//")
[ -z "$DW_VERSION" ] && echo "Please fix me! I can't determine the current DataWave version" && exit 1

####################################################################################
########################## DEPLOY DATAWAVE  ########################################

echo ""
echo "Deploying Web Service and Ingest. (Previous deployments will be deleted)"
echo ""
echo "DataWave Version: $DW_VERSION"
echo "     Web Service: $WEBSERVICE_TAR"
echo "          Ingest: $INGEST_TAR"
echo ""

cp $WEBSERVICE_TAR $DEPLOY_HOME/
cp $INGEST_TAR $DEPLOY_HOME/

cd $DEPLOY_HOME/
unlink $INGEST_SYMLINK
unlink $WEBSERVICE_SYMLINK
rm -rf datawave-dev-$DW_VERSION/
rm -rf datawave-web-service-$DW_VERSION/
tar xf datawave-dev-$DW_VERSION-dist.tar.gz
tar xf datawave-web-service-$DW_VERSION-dev.tar.gz
ln -s datawave-dev-$DW_VERSION $INGEST_SYMLINK
ln -s datawave-web-service-$DW_VERSION $WEBSERVICE_SYMLINK
rm -rf wildfly-9.0.1.Final/
tar xf wildfly-9.0.1.Final.tar.gz 

# Copy $DEPLOY_HOME/reset-accumulo.sh into place. If you ever edit 
# the in-place copy (ie, the one in datawave-ingest/bin/ingest/) and
# you want to preserve those edits, make sure you copy the file
# back to $DEPLOY_HOME/reset-accumulo.sh. Otherwise, you'll lose
# your changes next time this script runs
[ -f $ACCUMULO_RESET_SCRIPT ] && cp $DEPLOY_HOME/reset-accumulo.sh $DEPLOY_HOME/datawave-ingest/bin/ingest
[ -f $ACCUMULO_RESET_SCRIPT ] && chmod 755 $DEPLOY_HOME/$INGEST_SYMLINK/bin/ingest/reset-accumulo.sh

# Setup Wildfly
cd $DEPLOY_HOME/datawave-webservice/
./setup-wildfly.sh --file add-datawave-configuration.cli

# If necessary, move native libs back into place and start hadoop
HADOOP_PID=$(/usr/bin/jps -lm | grep namenode\.NameNode | cut -d' ' -f1)
[ "$NATIVE_BACKUP_ENABLED" = true ] && cp $HADOOP_NATIVE_BACKUP/* $HADOOP_NATIVE/
[ -z "$HADOOP_PID" ] && cd $HADOOP_HOME/sbin && ./start-all.sh && ./mr-jobhistory-daemon.sh start historyserver

echo ""
echo ""
read -r -p "Refresh Accumulo VFS classpaths in HDFS? [y/n]: " USER_RESPONSE
if [[ "$USER_RESPONSE" == "y" ]]; then
   # Ensure that HDFS is out of safemode
   hdfs dfsadmin -safemode wait
   hdfs dfs -mkdir /accumulo-datawave-classpath
#   hdfs dfs -mkdir /accumulo-system-classpath
   hdfs dfs -rm -r /accumulo-datawave-classpath/*
#   hdfs dfs -rm -r /accumulo-system-classpath/*
   hdfs dfs -put -f $DEPLOY_HOME/$INGEST_SYMLINK/accumulo-warehouse/lib/*.jar /accumulo-datawave-classpath
   hdfs dfs -put -f $DEPLOY_HOME/$INGEST_SYMLINK/accumulo-warehouse/lib/ext/*.jar /accumulo-datawave-classpath
#   hdfs dfs -rm -r /accumulo-datawave-classpath/joda*
#   hdfs dfs -put -f $DEPLOY_HOME/$INGEST_SYMLINK/lib/joda* /accumulo-system-classpath
fi

#------------- Post-deploy, in-place edits should go here if needed ---------------------------------

# Hack to use DefaultCodec rather than SnappyCodec for bulk/oneHr ingest,
# since my libhadoop was built without snappy support...
#SED="s/SnappyCodec/DefaultCodec/g"
#find $DEPLOY_HOME/$INGEST_SYMLINK/ -type f -name "ingest-env.sh" -exec sed -i ''$SED'' {} \;

#----------------------------------------------------------------------------------------------------

########################## START REMAINING SERVICES  #######################################

ACCUMULO_PID=$(/usr/bin/jps -lm | grep 'org.apache.accumulo.start.Main master' | cut -d' ' -f1)
ZK_PID=$(/usr/bin/jps -lm | grep QuorumPeerMain | cut -d' ' -f1)
[ -z "$ZK_PID" ] && cd $ZOOKEEPER_HOME/bin && ./zkServer.sh start
[ -z "$ACCUMULO_PID" ] && cd $ACCUMULO_HOME/bin && ./start-all.sh

USER_RESPONSE="n"
INGEST_PID=$(/usr/bin/jps -lm | grep 'nsa.datawave.util.flag.FlagMaker' | grep 'config/FiveMinFlagMaker.xml' | cut -d' ' -f1)
#INGEST_PID=$(/usr/bin/jps -lm | grep 'jar nsa.datawave.poller.Poller' | cut -d' ' -f1)
[ -z "$INGEST_PID" ] && read -r -p "Do you want to start ingest? [y/n]: " USER_RESPONSE
if [[ "$USER_RESPONSE" == "y" ]]; then
   echo "" 
   echo "" 
   USER_RESPONSE="n"
   [ -f $ACCUMULO_RESET_SCRIPT ] && read -r -p "Would you like to execute 'reset-accumulo.sh' before starting ingest? [y/n]: " USER_RESPONSE
   [ "$USER_RESPONSE" == "y" ] && cd $DEPLOY_HOME/$INGEST_SYMLINK/bin/ingest && ./reset-accumulo.sh 
   echo ""
   cd $DEPLOY_HOME/$INGEST_SYMLINK/bin/system && ./start-all.sh -allforce
fi

cd $DEPLOY_HOME
echo ""
echo "Datawave Deployed"
echo ""

USER_RESPONSE="n"
INGEST_PID=$(/usr/bin/jps -lm | grep 'nsa.datawave.util.flag.FlagMaker' | grep 'config/FiveMinFlagMaker.xml' | cut -d' ' -f1)
TEST_DATA_PROCESSED=""
[ ! -z "$INGEST_PID" ] && read -r -p "Would you like to send test data through ingest? [y/n]: " USER_RESPONSE
if [[ "$USER_RESPONSE" == "y" ]]; then
   for (( count=0; count < ${#POLLER_INPUT_FILES[@]}; count=$((count + 1)) )); do
      pollerdata=${POLLER_INPUT_FILES[$count]}
      pollerdir=${POLLER_INPUT_DIRS[$count]}
      if [[ "$pollerdata" != "" ]] && [[ -f $pollerdata ]]; then
         if [[ "$pollerdir" != "" ]] && [[ -d $pollerdir ]]; then
            cp $pollerdata $pollerdir
            TEST_DATA_PROCESSED="y"
         else
            echo "" 
            echo "$pollerdir does not exist!!!!!!" && sleep 5
            echo "" 
         fi
      else
         echo "" 
         echo "$pollerdata does not exist!!!!!!" && sleep 5
         echo ""
      fi
   done

   NOW=$(date +%Y%m%d%H%M%S) 
   for (( count=0; count < ${#HDFS_INPUT_FILES[@]}; count=$((count + 1)) )); do
      hdfsdata=${HDFS_INPUT_FILES[$count]}
      hdfsdir=${HDFS_INPUT_DIRS[$count]}
      if [[ "$hdfsdata" != "" ]] && [[ -f $hdfsdata ]]; then
         # Ensure the file goes into in HDFS with a unique file name
         # to guarantee 'freshness' and for easier tracking
         hdfsfile=${hdfsdata##*/}
         hdfsfileUnique="$hdfsfile.$NOW"
         hdfs dfs -put $hdfsdata $hdfsdir/$hdfsfileUnique
         TEST_DATA_PROCESSED="y"
      else
         echo ""
         echo "$hdfsdata does not exist!!!!!!!" && sleep 5
         echo ""
      fi
   done
fi

cd $WILDFLY_HOME/bin
nohup ./standalone.sh -c standalone-full.xml &

cd $DEPLOY_HOME

echo ""
USER_RESPONSE="n"
[ ! -z "$INGEST_PID" ] && [ ! -z "$TEST_DATA_PROCESSED" ] && read -r -p "Would you like to tail ingest logs? [y/n]: " USER_RESPONSE
[ "$USER_RESPONSE" == "y" ] && tail -f $DW_INGEST_LOG_DIR/*.log

echo ""
echo "Verify running services... "
echo ""
/usr/bin/jps -lm
echo ""

