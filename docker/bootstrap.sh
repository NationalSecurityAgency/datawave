echo "Creating .env file..."
echo

# Ensure that permissions are set correctly for the config files
chmod -R 755 config pki rabbitmq-config
chmod -R 755 config pki rabbitmq-query-config

DW_HOSTNAME=$(hostname)
DW_HOSTNAME=${DW_HOSTNAME%%.*}
DW_HOST_FQDN=$(hostname -f)

# If the hostname matches the fqdn, leave the fqdn unset
if [[ "${DW_HOST_FQDN}" == "${DW_HOSTNAME}" ]]; then
   DW_HOST_FQDN="unused"
fi

DW_HOST_IP=${DW_HOST_IP:-$(hostname -i)}

if [ "$1" == "hybrid" ] ; then
   COMPOSE_PROFILES=""
   DW_ZOOKEEPER_HOST=${DW_HOSTNAME}
   DW_HADOOP_HOST=${DW_HOSTNAME}
   DW_YARN_HOST=${DW_HOSTNAME}
   HADOOP_CONF_SOURCE=${HADOOP_CONF_SOURCE:-${HADOOP_CONF_DIR}}
   if [[ -z "${HADOOP_CONF_SOURCE}" ]]; then
      echo "HADOOP_CONF_DIR or HADOOP_CONF_SOURCE must be set in hybrid mode" >&2
      exit 1
   fi
else
   COMPOSE_PROFILES=datawave-stack
   DW_ZOOKEEPER_HOST=zookeeper
   DW_HADOOP_HOST=hdfs-nn
   DW_YARN_HOST=yarn-rm
   HADOOP_CONF_SOURCE=${HADOOP_CONF_SOURCE:-./stack}
fi

ENV_CONF="\
# Enables the reusable Hadoop/Accumulo stack and DataWave fixture loader
# Note: More than one profile may be set.
COMPOSE_PROFILES=\"${COMPOSE_PROFILES}\"

# These environment variables are used to create extra hosts which
# allow containers to route to services running on the host in hybrid mode.
DW_HOSTNAME=\"${DW_HOSTNAME}\"
DW_HOST_FQDN=\"${DW_HOST_FQDN}\"
DW_HOST_IP=\"${DW_HOST_IP}\"

# Backend service locations for the selected deployment mode.
DW_ZOOKEEPER_HOST=\"${DW_ZOOKEEPER_HOST}\"
DW_HADOOP_HOST=\"${DW_HADOOP_HOST}\"
DW_YARN_HOST=\"${DW_YARN_HOST}\"
HADOOP_CONF_SOURCE=\"${HADOOP_CONF_SOURCE}\"
"

# Write .env file using our settings in ENV_CONF
if [ ! -z "${ENV_CONF}" ] ; then 
   echo "${ENV_CONF}" > ./.env || ( fatal "Failed to write .env" && exit 1 )
else
   warn "No .env content defined! :("
fi

cat .env
