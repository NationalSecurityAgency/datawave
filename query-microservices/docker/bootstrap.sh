echo "Creating .env file..."
ENV_CONF="\
HOSTNAME=$(hostname)
HOST_FQDN=$(hostname -f)
HOST_IP=$(hostname -i)"

# Write .env file using our settings in ENV_CONF
if [ ! -z "${ENV_CONF}" ] ; then 
   echo "${ENV_CONF}" > ./.env || fatal "Failed to write .env"
else
   warn "No .env content defined! :("
fi
