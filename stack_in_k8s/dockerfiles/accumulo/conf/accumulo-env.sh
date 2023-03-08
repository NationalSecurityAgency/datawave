#! /usr/bin/env bash

# Copyright 2020 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if [[ -z $HADOOP_HOME ]] ; then
	test -z "$HADOOP_PREFIX"      && export HADOOP_PREFIX=/opt/hadoop
else
	HADOOP_PREFIX="$HADOOP_HOME"
	unset HADOOP_HOME
fi

test -z "$HADOOP_CONF_DIR"       && export HADOOP_CONF_DIR="$HADOOP_PREFIX/etc/hadoop"
test -z "$JAVA_HOME"             && export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/
test -z "$ZOOKEEPER_HOME"        && export ZOOKEEPER_HOME=/opt/zookeeper
test -z "$ACCUMULO_LOG_DIR"      && export ACCUMULO_LOG_DIR=$ACCUMULO_HOME/logs

if [[ -f ${ACCUMULO_CONF_DIR}/accumulo.policy ]]; then
	POLICY="-Djava.security.manager -Djava.security.policy=${ACCUMULO_CONF_DIR}/accumulo.policy"
fi

test -z "$ACCUMULO_TSERVER_OPTS" && export ACCUMULO_TSERVER_OPTS="${POLICY} -Xmx128m -Xms128m "
test -z "$ACCUMULO_MASTER_OPTS"  && export ACCUMULO_MASTER_OPTS="${POLICY} -Xmx128m -Xms128m"
test -z "$ACCUMULO_MONITOR_OPTS" && export ACCUMULO_MONITOR_OPTS="${POLICY} -Xmx64m -Xms64m"
test -z "$ACCUMULO_GC_OPTS"      && export ACCUMULO_GC_OPTS="-Xmx64m -Xms64m"
test -z "$ACCUMULO_SHELL_OPTS"   && export ACCUMULO_SHELL_OPTS="-Xmx128m -Xms64m"
test -z "$ACCUMULO_GENERAL_OPTS" && export ACCUMULO_GENERAL_OPTS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -Djava.net.preferIPv4Stack=true -XX:+CMSClassUnloadingEnabled"
test -z "$ACCUMULO_OTHER_OPTS"   && export ACCUMULO_OTHER_OPTS="-Xmx128m -Xms64m"
test -z "${ACCUMULO_PID_DIR}"    && export ACCUMULO_PID_DIR="${ACCUMULO_HOME}/run"

# what to do when the JVM runs out of heap memory
export ACCUMULO_KILL_CMD='kill -9 %p'

### Optionally look for hadoop and accumulo native libraries for your
### platform in additional directories. (Use DYLD_LIBRARY_PATH on Mac OS X.)
export LD_LIBRARY_PATH=${HADOOP_PREFIX}/lib/native/:${LD_LIBRARY_PATH}

# Should the monitor bind to all network interfaces -- default: false
export ACCUMULO_MONITOR_BIND_ALL="true"

# Should process be automatically restarted
# export ACCUMULO_WATCHER="true"
# What settings should we use for the watcher, if enabled
export UNEXPECTED_TIMESPAN="3600"
export UNEXPECTED_RETRIES="2"
export OOM_TIMESPAN="3600"
export OOM_RETRIES="5"
export ZKLOCK_TIMESPAN="600"
export ZKLOCK_RETRIES="5"

# The number of .out and .err files per process to retain
# export ACCUMULO_NUM_OUT_FILES=5

export NUM_TSERVERS=1
