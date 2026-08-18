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
else
   COMPOSE_PROFILES=datawave-stack
   DW_ZOOKEEPER_HOST=zookeeper
   DW_HADOOP_HOST=hdfs-nn
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
"

# Write .env file using our settings in ENV_CONF
if [ ! -z "${ENV_CONF}" ] ; then 
   echo "${ENV_CONF}" > ./.env || ( fatal "Failed to write .env" && exit 1 )
else
   warn "No .env content defined! :("
fi

cat .env
