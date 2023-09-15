echo "Creating .env file..."
echo

# Ensure that permissions are set correctly for the config files
chomd -R 755 config pki rabbitmq-config

DW_HOSTNAME=$(hostname)
DW_HOSTNAME=${DW_HOSTNAME%%.*}
DW_HOST_FQDN=$(hostname -f)

# If the hostname matches the fqdn, leave the fqdn unset
if [[ "${DW_HOST_FQDN}" == "${DW_HOSTNAME}" ]]; then
   DW_HOST_FQDN="unused"
fi

DW_HOST_IP=$(hostname -i)

if [ "$1" == "hybrid" ] ; then
   COMPOSE_PROFILES=""
   DW_ZOOKEEPER_HOST=${DW_HOSTNAME}
   DW_HADOOP_HOST=${DW_HOSTNAME}
else
   COMPOSE_PROFILES=quickstart
   DW_ZOOKEEPER_HOST=quickstart
   DW_HADOOP_HOST=quickstart
fi

ENV_CONF="\
# If set to quickstart, enables the quickstart container
# Note: More than one profile may be set.
COMPOSE_PROFILES=\"${COMPOSE_PROFILES}\"

# These environment variables are used to create extra hosts which
# allow containers to route to the host quickstart deployment.
# The extra hosts aren't used when deploying the docker quickstart,
# but the variables still need to be set for the compose file to be valid.
DW_HOSTNAME=\"${DW_HOSTNAME}\"
DW_HOST_FQDN=\"${DW_HOST_FQDN}\"
DW_HOST_IP=\"${DW_HOST_IP}\"

# These environment variables must be set when running the quickstart
# from the host machine in hybrid mode.
DW_ZOOKEEPER_HOST=\"${DW_ZOOKEEPER_HOST}\"
DW_HADOOP_HOST=\"${DW_HADOOP_HOST}\"
"

# Write .env file using our settings in ENV_CONF
if [ ! -z "${ENV_CONF}" ] ; then 
   echo "${ENV_CONF}" > ./.env || fatal "Failed to write .env"
else
   warn "No .env content defined! :("
fi

cat .env
