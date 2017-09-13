BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${BIN_DIR}/logging.sh"

function register() {
   local servicename="$1"
   local servicescript="${DW_CLOUD_HOME}/bin/services/${servicename}/bootstrap.sh"
   #
   # Here we create & maintain a simple registry of service names, so that services can
   # be manipulated without hard-coding their distinct names in our "*All" functions
   # below, etc...
   #
   # Note that the service "contract" requires the following:
   #
   #   (1) A "bin/services/{servicename}/bootstrap.sh" script must exist and provide any
   #       environment variables and configs that are required for bootstrapping.
   #
   #   (2) More specifically, the bootstrap.sh script must implement the following wrapper
   #       functions, which can be used to control and inspect the service as needed:
   #
   #       {servicename}Start       - Starts the service
   #       {servicename}Stop        - Stops the service
   #       {servicename}Status      - Current status of the service. PIDs if running
   #       {servicename}Install     - Installs the service
   #       {servicename}Uninstall   - Uninstalls the service
   #       {servicename}IsRunning   - Returns 0 if running, non-zero otherwise
   #       {servicename}IsInstalled - Returns 0 if installed, non-zero otherwise
   #       {servicename}Printenv    - Display current state of the service config
   #
   if [ -z "${servicename}" ] ; then
      error "Registration failed: service name was null"
      return 1
   fi

   if [ ! -f "${servicescript}" ] ; then
      error "Registration failed: ${servicescript} doesn't exist"
      return 1
   fi

   if [ ! -z "$( echo "${DW_CLOUD_SERVICES}" | grep "${servicename}" )" ] ; then
      return 1 # servicename is already registered
   fi

   # Source the service script
   source "${servicescript}"

   # TODO: should probably verify/assert that all required functions have been implemented

   # Finally, add service name to our list
   DW_CLOUD_SERVICES="${DW_CLOUD_SERVICES} ${servicename}"
}

function resetDataWaveEnvironment() {
   DW_CLOUD_SERVICES=""
   source "${DW_CLOUD_HOME}/bin/env.sh"
}

function askYesNo() {
   # First, check the status of DEFAULT_YN_RESPONSE to see if the
   # user wants to avoid prompts of any kind. The only valid values
   # are either Y or N, case-insensitive...
   if [ -n "${DEFAULT_YN_RESPONSE}" ] ; then
       local defaultResponse="$( echo "$DEFAULT_YN_RESPONSE" | tr '[:upper:]' '[:lower:]' )"
       [ "${defaultResponse}" == "y" ] && return 0
       [ "${defaultResponse}" == "n" ] && return 1
   fi
   # Get the user's 'yes|y' / 'no|n' reply to the given question
   # (Keeps asking until a valid response is given)
   echo
   YN_MSG="$1"
   while true; do
      read -r -p "${YN_MSG} [y/n]: " USER_REPLY
      case "$( echo "$USER_REPLY" | tr '[:upper:]' '[:lower:]' )" in
         y|yes) echo && return 0 ;;
         n|no) echo && return 1 ;;
      esac
      echo " - Invalid response"
   done
}

function downloadTarball() {
   # Downloads the specified tarball, if it doesn't already exist
   local uri="$1"
   local tarballdir="$2"
   tarball="$( basename ${uri} )"
   if [ ! -f "${tarballdir}/${tarball}" ] ; then
      $( cd "${tarballdir}" && wget "${uri}" ) || error "Failed to wget '${uri}'"
   fi
}

function downloadOracleJava8Tarball() {
    local tarball="$1"
    local tarballdir="$2"

    local oracle_url="http://www.oracle.com"
    local jdk_url1="$oracle_url/technetwork/java/javase/downloads/index.html"
    local jdk_url2=$(curl -s "${jdk_url1}" | egrep -o "\/technetwork\/java/\javase\/downloads\/jdk8-downloads-.+?\.html" | head -1 | cut -d '"' -f 1)

    [[ -z "$jdk_url2" ]] && error "Could not get jdk download url - $jdk_url1"

    local jdk_url3="${oracle_url}${jdk_url2}"
    local jdk_url4=$(curl -s $jdk_url3 | egrep -o "http\:\/\/download.oracle\.com\/otn-pub\/java\/jdk\/8u[0-9]+\-(.*)+\/jdk-8u[0-9]+(.*)linux-x64.tar.gz")

    if [ ! -f "${tarballdir}/${tarball}" ] ; then
        $( cd "${tarballdir}" && wget --no-cookies --no-check-certificate \
             --header "Cookie: oraclelicense=accept-securebackup-cookie" \
             $jdk_url4 -O $tarball ) || error "Failed to wget '${jdk_url4}'"
    fi
}

function writeSiteXml() {
   # Writes *-site.xml files, such as hdfs-site.xml, accumulo-site.xml, etc...

   local sitefile="$1" # The file name to write
   local siteconf="$2" # The property name/value pairs to write

   # read the "name value" siteconf properties, one line at a time
   printf '%s\n' "$siteconf" | ( while IFS= read -r nameval ; do
       # parse the name and value from the line
       local name=${nameval% *}
       local value=${nameval##* }
       # concatenate the xml into a big blob
       local xml=${xml}$(printf "\n   <property>\n      <name>$name</name>\n      <value>$value</value>\n   </property>\n")
   done
   # write the blob to file...
   printf "<configuration>${xml}\n</configuration>" > ${sitefile} )
}

function statusAll() {
   # Gets the status of all registered services
   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      ${servicename}Status
   done
}

function startAll() {
   # Starts all registered services
   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      if ${servicename}IsInstalled ; then
         ${servicename}IsRunning || ${servicename}Start
      else
         info "${servicename} service is not installed"
      fi
   done
}

function stopAll() {
   # Stops all registered services
   if ! servicesAreRunning ; then
      echo "No services are currently running"
      return 1
   fi
   local services=(${DW_CLOUD_SERVICES})
   # Loop in reverse order for stopping services
   # In other words, order of registration matters.
   # e.g., we don't want to stop Hadoop *before* Accumulo.
   for (( idx=${#services[@]}-1 ; idx>=0 ; idx-- )) ; do
      ${services[idx]}Stop
   done
}

function allServicesAreInstalled() {
   local services=(${DW_CLOUD_SERVICES})
   local -i numServices=${#services[@]}
   local -i servicesNotInstalled=0
   for servicename in "${services[@]}" ; do
      if ! ${servicename}IsInstalled ; then
         servicesNotInstalled=$(( servicesNotInstalled + 1 ))
         DW_CLOUD_SERVICES_NOT_INSTALLED="${DW_CLOUD_SERVICES_NOT_INSTALLED} ${servicename}"
      fi
   done
   [ ${servicesNotInstalled} -gt 0 ] && return 1 || return 0
}

function isInteractiveShell() {
   [ -z "$PS1" ] && return 1 || return 0
}

function isLoginShell() {
   local shell="$0"
   [[ "${shell:0:1}" != "-" ]] && return 1 || return 0
}

function promptForInstallation() {
   installInProgressFlag="${DW_CLOUD_HOME}/.install-in-progress"

   [ -f "${installInProgressFlag}" ] && return

   if ! allServicesAreInstalled ; then
      if [ "${DW_REDEPLOY_IN_PROGRESS}" == true ] ; then
         # Auto-install on redeploy
         installMissingServices
      else
         if askYesNo "Install the following services: ${DW_CLOUD_SERVICES_NOT_INSTALLED} ?" ; then
            installMissingServices
         fi
      fi
   fi
}

function installMissingServices() {
   if ! allServicesAreInstalled ; then
      date > "${installInProgressFlag}"
      local services=(${DW_CLOUD_SERVICES_NOT_INSTALLED})
      for servicename in "${services[@]}" ; do
         ${servicename}IsInstalled || ${servicename}Install
      done
      rm -f "${installInProgressFlag}"
   fi
}

function installAll() {
   # Installs all registered services
   if servicesAreRunning ; then
      echo "Services are currently running"
      statusAll
      return 1
   fi
   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      ${servicename}Install
   done
}

function uninstallAll() {
   # All data will be removed by default. To keep data, add '--keep-data' argument

   # Uninstalls all registered services. 
   if servicesAreRunning ; then
      echo "Stop running services before uninstalling!"
      statusAll
      return 1
   fi

   askYesNo "Uninstalling everything under '${DW_CLOUD_HOME}'. This can not be undone.
Continue?" || exit 1

   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      ${servicename}Uninstall
   done

   if [[ -z  "$1" || "$1" != "--keep-data" ]] ; then
      # Remove data
      [ -d "${DW_CLOUD_DATA}" ] && rm -rf "${DW_CLOUD_DATA}" && info "Removed ${DW_CLOUD_DATA}"
   fi
}

function printenvAll() {
   echo
   echo "DW Cloud Environment"
   echo
   ( set -o posix ; set ) | grep "DW_CLOUD"
   echo
   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      ${servicename}Printenv
   done
}

function assertCreateDir() {
   [[ $# -eq 0 || -z "$1" ]] && fatal "[${FUNCNAME[0]}] Directory parameter cannot be empty"
   [ -d $1 ] && warn "[${FUNCNAME[0]}] already exists!" && return
   mkdir -p "$1" && info "Created directory: $1"
   [ ! -d "$1" ] && fatal "[${FUNCNAME[0]}] configured based directory $1 does not exist"
}

function sshIsInstalled() {
   [ -z "$( which ssh )" ] && return 1
   [ -z "$( which sshd )" ] && return 1
   return 0
}

function servicesAreRunning() {
   # Returns 0, if any registered services are running.
   # Returns non-zero, if no registered services are running
   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      ${servicename}IsRunning && return 0
   done
   return 1 # Nothing running
}
