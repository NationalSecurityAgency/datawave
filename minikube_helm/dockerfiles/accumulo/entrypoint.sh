#!/bin/bash



test -z "${ACCUMULO_INSTANCE_NAME}" && ACCUMULO_INSTANCE_NAME="dev"

if [ "$1" = "accumulo" ] && [ "$2" = "master" ]; then
	# Try to find desired root password from trace config
	TRACE_USER=root
	CLIENT_USERNAME=root

	# Try to find desired root password from environment variable
	[ ! -z "${ACCUMULO_ROOT_PASSWORD}" ] && PASSWORD="${ACCUMULO_ROOT_PASSWORD}"

	if [ -z "${PASSWORD}" ]; then
		echo "Unable to determine what the Accumulo root user's password should be."
		echo "Please set:"
		echo "- ACCUMULO_ROOT_PASSWORD environment variable"
		exit 1
	fi

	# If possible, wait until all the HDFS instances that Accumulo will be using are available i.e. not in Safe Mode and directory is writeable
	ACCUMULO_VOLUMES="hdfs://hdfs-nn:9000/accumulo"
	if [ ! -z "${ACCUMULO_VOLUMES}" ]; then
		HADOOP_CLASSPATH="${ACCUMULO_CONF_DIR}:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/client/*:${HADOOP_HOME}/share/hadoop/common/lib/*"

		until [ "${ALL_VOLUMES_READY}" == "true" ] || [ $(( ATTEMPTS++ )) -gt 6 ]; do
			echo "$(date) - Waiting for all HDFS instances to be ready..."
			ALL_VOLUMES_READY="true"
			for ACCUMULO_VOLUME in ${ACCUMULO_VOLUMES//,/ }; do
				SAFE_MODE_CHECK="OFF"
				SAFE_MODE_CHECK_OUTPUT=$(java -cp ${HADOOP_CLASSPATH} org.apache.hadoop.hdfs.tools.DFSAdmin --fs ${ACCUMULO_VOLUME} -safemode get)
				echo ${SAFE_MODE_CHECK_OUTPUT} | grep -q "Safe mode is OFF"
				[ "$?" != "0" ] && ALL_VOLUMES_READY="false" && SAFE_MODE_CHECK="ON"

				WRITE_CHECK="writeable"
				java -cp ${HADOOP_CLASSPATH} org.apache.hadoop.fs.FsShell -mkdir -p ${ACCUMULO_VOLUME}
				java -cp ${HADOOP_CLASSPATH} org.apache.hadoop.fs.FsShell -test -w ${ACCUMULO_VOLUME}
				[ "$?" != "0" ] && ALL_VOLUMES_READY="false" && WRITE_CHECK="not writeable"

				echo ${ACCUMULO_VOLUME} "- Safe mode is" ${SAFE_MODE_CHECK} "-" ${WRITE_CHECK}
			done
			[ "${ALL_VOLUMES_READY}" == "true" ] || sleep 10
		done
		[ "${ALL_VOLUMES_READY}" != "true" ] && echo "$(date) - ERROR: Timed out waiting for HDFS instances to be ready..." && exit 1
	fi

	echo "Initializing Accumulo..."
	ALREADY_INIT=`/opt/accumulo/bin/accumulo org.apache.accumulo.server.util.ListInstances | grep ${ACCUMULO_INSTANCE_NAME}|wc -l`
	echo "Checked init. Was: ${ALREADY_INIT}"
	[ $ALREADY_INIT -eq 0 ] && echo "Initializing" && accumulo init --instance-name ${ACCUMULO_INSTANCE_NAME} --password ${PASSWORD}
fi

exec /usr/bin/dumb-init -- "$@"
