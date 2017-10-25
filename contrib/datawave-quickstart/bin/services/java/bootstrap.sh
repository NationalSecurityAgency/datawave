# Sourced by env.sh

DW_JAVA_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# You may override DW_JAVA_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
DW_JAVA_DIST_URI="${DW_JAVA_DIST_URI:-http://Doesnt-matter.See-the-downloadOracleJava8Tarball-method/jdk-8-linux-x64.tar.gz}"
DW_JAVA_DIST="$( basename "${DW_JAVA_DIST_URI}" )"
DW_JAVA_BASEDIR="jdk-8-linux-x64"
DW_JAVA_SYMLINK="java"

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

function bootstrapEmbeddedJava() {
    if [ ! -f "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_DIST}" ] ; then
        info "JDK 1.8 was not detected. Attempting to bootstrap a dedicated install..."
        if [[ "${DW_JAVA_DIST_URI}" == file:///* && -f "${DW_JAVA_DIST_URI#file://}" ]] ; then
            # We're configured for a local tarball copy
            downloadTarball "${DW_JAVA_DIST_URI}" "${DW_JAVA_SERVICE_DIR}"
        else
            # We'll need to grab one remotely from Oracle
            downloadOracleJava8Tarball "${DW_JAVA_DIST}" "${DW_JAVA_SERVICE_DIR}"
        fi
    fi
    export JAVA_HOME="${DW_CLOUD_HOME}/${DW_JAVA_SYMLINK}"
    export PATH="${JAVA_HOME}/bin:${PATH}"
}

function embeddedJavaIsInstalled() {
    [ -f "${DW_CLOUD_HOME}/${DW_JAVA_SYMLINK}/bin/javac" ] && bootstrapEmbeddedJava && return 0
    [ -f "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_BASEDIR}/bin/javac" ] && bootstrapEmbeddedJava && return 0
    return 1
}

function javaIsInstalled() {
    embeddedJavaIsInstalled && return 0

    if [[ -n "${JAVA_HOME}" ]] && [[ -x "${JAVA_HOME}/bin/javac" ]] ; then
        # JDK already set in this environment. If it's 1.8 then we're good to go
        [[ -n "$( ${JAVA_HOME}/bin/javac -version 2>&1 | grep 'javac 1.8' )" ]] && return 0
    fi

    # If we're here then we need to bootstrap a dedicated JDK
    bootstrapEmbeddedJava

    return 1
}

function javaInstall() {
    javaIsInstalled && info "Java is already installed" && return 1
    [ ! -f "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_DIST}" ] && error "Java tarball not found" && return 1
    ! mkdir "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_BASEDIR}" && error "Failed to create Java base directory" && return 1
    tar xf "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_DIST}" -C "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_BASEDIR}" --strip-components=1
    $( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/java/${DW_JAVA_BASEDIR}" "${DW_JAVA_SYMLINK}" )
    ! javaIsInstalled && error "Java was not installed" && return 1
    info "Java installed"
}

function javaUninstall() {
    if embeddedJavaIsInstalled ; then
        if [ -L "${DW_CLOUD_HOME}/${DW_JAVA_SYMLINK}" ] ; then
            ( cd "${DW_CLOUD_HOME}" && unlink "${DW_JAVA_SYMLINK}" ) || error "Failed to remove Java symlink"
        fi

        if [ -d "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_BASEDIR}" ] ; then
            rm -rf "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_BASEDIR}"
        fi

        ! embeddedJavaIsInstalled && info "Java uninstalled" || error "Java uninstall failed"
    else
        info "Java not installed. Nothing to do"
    fi

    [ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG}" ] && rm -f "${DW_JAVA_SERVICE_DIR}"/*.tar.gz
}

function javaIsRunning() {
    return 1 # No op
}

function javaStart() {
    return 0 # No op
}

function javaStop() {
    return 0 # No op
}

function javaStatus() {
    return 0 # No op
}

function javaPrintenv() {
   echo
   echo "Java Environment"
   echo
   ( set -o posix ; set ) | grep "JAVA_"
   echo
}

function javaPidList() {
   return 0 # No op
}

# Eager-loading here since Java is a common dependency for everything,
# as opposed to lazy-loading like the other services...

! javaIsInstalled && javaInstall
