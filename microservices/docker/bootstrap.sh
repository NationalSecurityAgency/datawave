echo "Creating .env file..."

DW_HOSTNAME=$(hostname)
DW_HOSTNAME=${DW_HOSTNAME%%.*}

ENV_CONF="\
DW_HOSTNAME=${DW_HOSTNAME}
DW_HOST_FQDN=$(hostname -f)
DW_HOST_IP=$(hostname -i)"

# Write .env file using our settings in ENV_CONF
if [ ! -z "${ENV_CONF}" ] ; then 
   echo "${ENV_CONF}" > ./.env || fatal "Failed to write .env"
else
   warn "No .env content defined! :("
fi
