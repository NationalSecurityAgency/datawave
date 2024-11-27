BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${BIN_DIR}/logging.sh"
source "${BIN_DIR}/query.sh"
source "${BIN_DIR}/modification.sh"

# Upon uninstall, tarballs will be preserved in place by default.
# To remove them, use DW_UNINSTALL_RM_BINARIES_FLAG_*

DW_UNINSTALL_RM_BINARIES_FLAG_LONG="--remove-binaries"
DW_UNINSTALL_RM_BINARIES_FLAG_SHORT="-rb"

function register() {
   local servicename="$1"
   local servicescript="${DW_CLOUD_HOME}/bin/services/${servicename}/bootstrap.sh"

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

function resetQuickstartEnvironment() {

   if [ "${1}" == "--hard" ] ; then
       # The nuclear option...
       # kill -9 all running services
       allStop ${1}
       # Uninstall. All will be erased including downloaded tarballs, although you'll
       # still be able to opt out before the trigger is pulled here...
       allUninstall ${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}
   fi

   # Now we can re-source all bootstraps and thus re-register all configured services.
   # To accomplish that, all we need to do is unset DW_CLOUD_SERVICES and then source env.sh

   # Any missing tarballs will be re-downloaded (and/or re-copied if local) and, wrt DataWave tarballs, rebuilt

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

function verifyChecksum() {
  # $1 - distribution URI
  # $2 - service directory
  # $3 - tarball sha512 checksum
  local tarballName="$(basename "$1")"
  if [[ -f "$2/${tarballName}" ]]; then
      local calculatedChecksum="$( cd $2 && sha512sum ${tarballName} )"
      if [[ "${calculatedChecksum}" = "$3  ${tarballName}" ]] ; then
          info "Checksum verification success... [${tarballName}]"
      else
          error "------------------------------------------------------------------------"
          error "$(printRed "CHECKSUM MISMATCH") - Could not verify integrity of: ${tarballName}"
          error "------------------------------------------------------------------------"
          kill -INT $$
      fi
  fi
}

function downloadTarball() {
   # Downloads the specified tarball, if it doesn't already exist.
   # If you want to utilize a tarball from the local file system, simply use
   # "file:///absolute/path/to/filename" as the $1 (uri) arg
   local uri="$1"
   local tarballdir="$2"
   tarball="$( basename ${uri} )"
   if [ ! -f "${tarballdir}/${tarball}" ] ; then
      if [[ ${uri} == file://* ]] ; then
          $( cd "${tarballdir}" && cp  "${uri:7}" ./${tarball} ) || error "File copy failed for ${uri:7}"
      elif [[ ${uri} == http://* ]] ; then
          if ! askYesNo "Are you sure you want to download ${tarball} using HTTP? $( printRed "This can potentially be insecure." )" ; then
            kill -INT $$
          else
            $( cd "${tarballdir}" && wget ${DW_WGET_OPTS} "${uri}" )
          fi
      elif [[ ${uri} == https://* ]] ; then
          $( cd "${tarballdir}" && wget ${DW_WGET_OPTS} "${uri}" )
      else
        return 1
      fi
   fi
}

function downloadMavenTarball() {
   local pomFile="${DW_DATAWAVE_SOURCE_DIR:-$( cd "${DW_CLOUD_HOME}/../.." && pwd )}/pom.xml"
   local rootProject=":$1"
   local group="$2"
   local artifact="$3"
   local version="$4"
   local tarballdir="$5"
   tarball="${artifact}-${version}.tar.gz"
   if [ ! -f "${tarballdir}/${tarball}" ] ; then
      # download from maven repo
      output=$( mvn -f "${pomFile}" -pl "${rootProject}" -DremoteRepositories="remote-repo::default::${DW_MAVEN_REPOSITORY}" dependency:get -Dartifact="${group}:${artifact}:${version}" -Dpackaging="tar.gz" )
      retVal=$?
      if [ $retVal -ne 0 ]; then
         error "Failed to download ${tarball} via maven"
         error "$output"
         return $retVal
      else
         info "Downloaded ${artifact} via maven"
      fi

      # copy to specified directory
      output=$( mvn -f "${pomFile}" -pl "${rootProject}" dependency:copy -Dartifact="${group}:${artifact}:${version}:tar.gz" -DoutputDirectory="${tarballdir}" )
      retVal=$?
      if [ $retVal -ne 0 ]; then
         error "Failed to copy ${tarball} to ${tarballdir} via maven"
         error "$output"
         return $retVal
      fi
   fi
}

function writeSiteXml() {
   # Writes *-site.xml files, such as hdfs-site.xml, accumulo-site.xml, etc...

   local sitefile="$1" # The file name to write
   local siteconf="$2" # The property name/value pairs to write

   # read the "name value" siteconf properties, one line at a time
   printf '%s\n' "$siteconf" | ( while IFS= read -r nameval ; do
       # parse the name and value from the line
       local name=${nameval%% *}
       local value=${nameval#* }
       # concatenate the xml into a big blob
       local xml=${xml}$(printf "\n   <property>\n      <name>$name</name>\n      <value>$value</value>\n   </property>\n")
   done
   # write the blob to file...
   printf "<configuration>${xml}\n</configuration>" > ${sitefile} )
}

function allStatus() {
   # Gets the status of all registered services
   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      ${servicename}Status
   done
}

function allStart() {
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

function allDisplayBinaryInfo() {
   # Starts all registered services
   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      echo "========================================================="
      echo "$(printGreen "Binary info for ${servicename}")"
      ${servicename}DisplayBinaryInfo
   done
}

function allStop() {

   [[ "${1}" == "--hard" ]] && local kill9=true

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
      if [ "${kill9}" == true ] ; then
         local pidList="$( ${services[idx]}PidList )"
         if [ ! -z "${pidList}" ] ; then
            info "Killing ${services[idx]} services: ${pidList}"
            kill -9 ${pidList}
         fi
      else
         ${services[idx]}Stop
      fi
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

function allInstall() {
   # Installs all registered services
   if servicesAreRunning ; then
      echo "Services are currently running"
      allStatus
      return 1
   fi
   allDisplayBinaryInfo ; echo
   local services=(${DW_CLOUD_SERVICES})
   if ! checkBinaries ; then
      askYesNo "Please review the reported inconsistencies in your current setup.
Hint: Existing local binaries will take precedence over source binaries during installation.
If that's not what you want, please use $(printGreen "[servicename|all]Uninstall --remove-binaries") to remove local binaries first
Continue with installation?" || return 1
   fi
   for servicename in "${services[@]}" ; do
      ${servicename}Install
   done
}

function checkBinaries()  {
  local status=0
  # Dump source and local binaries into distinct arrays
  local sourceBinaries=()
  local localBinaries=()
  while IFS= read -r line; do
    if [[ "${line}" =~ ^Source:.* ]] ; then
      sourceBinaries+=("$(sed 's/.*: //g' <<<${line})")
    else
      localBinaries+=("$(sed 's/.*: //g' <<<${line})")
    fi
  done < <( allDisplayBinaryInfo | grep -E 'Source:|Local:' )

  # Compare each tarball pair
  for ((i=0; i<${#sourceBinaries[@]}; i++)); do
     local sbin="$(basename "${sourceBinaries[$i]}")"
     local lbin="$(basename "${localBinaries[$i]}")"
     if [[ "${sbin}" != "${lbin}" ]] && [[ ! "${lbin}" =~ .*Not\ loaded.* ]] ; then
        warn "Source and Local binaries are inconsistent: Source == ${sbin}, Local == ${lbin}"
        status=1
     fi
  done
  return ${status}
}

function getDataWaveVersion() {
  local pomFile="${DW_DATAWAVE_SOURCE_DIR}/pom.xml"
  if [[ -f "${pomFile}" ]]; then
     local version="$(grep -m 1 '<version>' "${pomFile}" 2>/dev/null | sed 's|.*<version>\(.*\)</version>.*|\1|')"
     [[ -z "${version}" ]] && return 1
  else
     return 1
  fi
  echo "${version}"
}

function allUninstall() {
   # ${DW_CLOUD_DATA} will be removed by default. To keep it, use '--keep-data' flag

   # Uninstalls all registered services. 
   if servicesAreRunning ; then
      echo "Stop running services before uninstalling!"
      allStatus
      return 1
   fi

   askYesNo "Uninstalling everything under '${DW_CLOUD_HOME}'. This can not be undone.
Continue?" || return 1

   local removeBinaries=false
   local keepData=false

   while [ "${1}" != "" ]; do
      case "${1}" in
         --keep-data)
            keepData=true
            ;;
         ${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}|${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT})
            removeBinaries=true
            ;;
         *)
            error "Invalid argument passed: ${1}" && return 1
       esac
       shift
   done

   local services=(${DW_CLOUD_SERVICES})
   for servicename in "${services[@]}" ; do
      if [ "${removeBinaries}" == true ] ; then
         ${servicename}Uninstall ${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}
      else
         ${servicename}Uninstall
      fi
   done

   if [[ "${keepData}" == false ]] ; then
      # Remove data
      [ -d "${DW_CLOUD_DATA}" ] && rm -rf "${DW_CLOUD_DATA}" && info "Removed ${DW_CLOUD_DATA}"
   fi
}

function allPrintenv() {
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
   [ ! -d "$1" ] && fatal "[${FUNCNAME[0]}] configured base directory $1 does not exist"
}

function sshIsInstalled() {
   [ -z "$( which ssh )" ] && return 1
   # Check "/usr/sbin/sshd" directly since on some systems, /usr/sbin isn't in the user's path so which won't find it.
   [ -z "$( which sshd )" ] && [ ! -x "/usr/sbin/sshd" ] && return 1
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

function jdkIsConfigured() {
   local javacBinary="$(which javac)"
   local requiredVersion="javac 11.0"
   local foundIt=""

   # Check JAVA_HOME
   if [[ -n "${JAVA_HOME}" ]] ; then
      foundIt="$( "${JAVA_HOME}"/bin/javac -version 2>&1 | grep "${requiredVersion}" )"
      if [[ -n "${foundIt}" ]] ; then
         # Ensure that PATH and JAVA_HOME are in agreement
         if [[ "$(readlink -f "${JAVA_HOME}"/bin/javac)" != "$(readlink -f "${javacBinary}")" ]] ; then
            export PATH="${JAVA_HOME}/bin:${PATH}"
         fi
         return 0
      fi
   fi

   # Check PATH
   if [ -n "${javacBinary}" ] ; then
      foundIt="$( "${javacBinary}" -version 2>&1 | grep "${requiredVersion}" )"
      if [[ -n "${foundIt}" ]] ; then
         # Ensure that JAVA_HOME is in agreement with javac path
         export JAVA_HOME="$(dirname $(dirname $(readlink -f "${javacBinary}")))"
         info "Found '${requiredVersion}', setting JAVA_HOME to '${JAVA_HOME}'"
         return 0
      fi
   fi

   error "'${requiredVersion}' not found. Please install/configure a compatible JDK"
   return 1
}
