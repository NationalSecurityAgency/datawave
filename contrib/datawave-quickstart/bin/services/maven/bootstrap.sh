# Sourced by env.sh

DW_MAVEN_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DW_MAVEN_VERSION="3.8.8"
# You may override DW_MAVEN_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
DW_MAVEN_DIST_URI="${DW_MAVEN_DIST_URI:-https://dlcdn.apache.org/maven/maven-3/${DW_MAVEN_VERSION}/binaries/apache-maven-${DW_MAVEN_VERSION}-bin.tar.gz}"
DW_MAVEN_DIST="$( basename "${DW_MAVEN_DIST_URI}" )"
DW_MAVEN_BASEDIR="maven-install"
DW_MAVEN_SYMLINK="maven"

function bootstrapEmbeddedMaven() {
    if [ ! -f "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_DIST}" ]; then
        info "Maven 3.x not detected. Attempting to bootstrap a dedicated install..."
        DW_MAVEN_DIST="$( { downloadTarball "${DW_MAVEN_DIST_URI}" "${DW_MAVEN_SERVICE_DIR}" || downloadMavenTarball "datawave-parent" "gov.nsa.datawave.quickstart" "maven" "${DW_MAVEN_VERSION}" "${DW_MAVEN_SERVICE_DIR}"; } && echo "${tarball}" )"
    fi

    export MAVEN_HOME="${DW_CLOUD_HOME}/${DW_MAVEN_SYMLINK}"
    export M2_HOME="${MAVEN_HOME}"
    export PATH="${MAVEN_HOME}/bin:${PATH}"
}

function embeddedMavenIsInstalled() {
    [ -f "${DW_CLOUD_HOME}/${DW_MAVEN_SYMLINK}/bin/mvn" ] && bootstrapEmbeddedMaven && return 0
    [ -f "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_BASEDIR}/bin/mvn" ] && bootstrapEmbeddedMaven && return 0
    return 1
}

function mavenIsInstalled() {
    embeddedMavenIsInstalled && return 0

    local mvncmd="$( which mvn 2> /dev/null )"

    if [[ -z "${mvncmd}" && -n "${MAVEN_HOME}" && -x "${MAVEN_HOME}/bin/mvn" ]] ; then
        mvncmd="${MAVEN_HOME}/bin/mvn"
    elif [[ -z "${mvncmd}" && -n "${M2_HOME}" && -x "${M2_HOME}/bin/mvn" ]] ; then
        mvncmd="${M2_HOME}/bin/mvn"
    fi

    if [ -n "${mvncmd}" ] ; then
        # Maven already set in this environment. If it's 3.x then we're good to go
        [[ -n "$( ${mvncmd} -version 2>&1 | grep 'Apache Maven 3' )" ]] && return 0
        warn "Maven installation detected, but not Apache Maven 3.x: '${mvncmd}'"
    fi

    # If we're here then we need to bootstrap a dedicated Maven 3.x
    bootstrapEmbeddedMaven

    return 1
}

function mavenInstall() {
    mavenIsInstalled && info "Maven is already installed" && return 1
    [ ! -f "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_DIST}" ] && error "Maven tarball not found" && return 1
    ! mkdir "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_BASEDIR}" && error "Failed to create Maven base directory" && return 1
    tar xf "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_DIST}" -C "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_BASEDIR}" --strip-components=1
    $( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/maven/${DW_MAVEN_BASEDIR}" "${DW_MAVEN_SYMLINK}" )
    ! mavenIsInstalled && error "Maven was not installed" && return 1
    info "Maven installed"
}

function mavenUninstall() {
    if embeddedMavenIsInstalled ; then
        if [ -L "${DW_CLOUD_HOME}/${DW_MAVEN_SYMLINK}" ] ; then
            ( cd "${DW_CLOUD_HOME}" && unlink "${DW_MAVEN_SYMLINK}" ) || error "Failed to remove Maven symlink"
        fi

        if [ -d "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_BASEDIR}" ] ; then
            rm -rf "${DW_MAVEN_SERVICE_DIR}/${DW_MAVEN_BASEDIR}"
        fi

        ! embeddedMavenIsInstalled && info "Maven uninstalled" || error "Maven uninstall failed"
    else
        info "Maven not installed. Nothing to do"
    fi

    [[ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}" || "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT}" ]] && rm -f "${DW_MAVEN_SERVICE_DIR}"/*.tar.gz
}

function mavenIsRunning() {
    return 1 # No op
}

function mavenStart() {
    return 0 # No op
}

function mavenStop() {
    return 0 # No op
}

function mavenStatus() {
    return 0 # No op
}

function mavenPidList() {

   return 0 # No op

}

function mavenPrintenv() {
   echo
   echo "Maven Environment"
   echo
   ( set -o posix ; set ) | grep -E "MAVEN_|M2_"
   echo
}

function mavenDisplayBinaryInfo() {
    echo "Source: ${DW_MAVEN_DIST}"
    local tarballName="$(basename "$DW_MAVEN_DIST")"
  if [[ -f "${DW_MAVEN_SERVICE_DIR}/${tarballName}" ]]; then
     echo " Local: ${DW_MAVEN_SERVICE_DIR}/${tarballName}"
  else
     echo " Local: Not loaded"
  fi
}

# Eager-loading here since Maven is required to build DataWave,
# as opposed to lazy-loading like the other services...

! mavenIsInstalled && mavenInstall
