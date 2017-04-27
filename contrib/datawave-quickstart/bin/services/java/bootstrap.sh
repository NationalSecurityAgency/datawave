# Sourced by env.sh

DW_JAVA_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DW_JAVA_DIST_URI="file://${DW_JAVA_SERVICE_DIR}/jdk-8-linux-x64.tar.gz"
DW_JAVA_DIST="$( basename "${DW_JAVA_DIST_URI}" )"
DW_JAVA_BASEDIR="jdk-8-linux-x64"
DW_JAVA_SYMLINK="java"

function bootstrapEmbeddedJava() {
    [ ! -f "${DW_JAVA_SERVICE_DIR}/${DW_JAVA_DIST}" ] \
    && info "JDK 1.8 was not detected. Attempting to bootstrap a dedicated install..." \
    && downloadOracleJava8Tarball "${DW_JAVA_DIST}" "${DW_JAVA_SERVICE_DIR}"

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

# Eager-loading here since Java is a common dependency for everything,
# as opposed to lazy-loading like the other services...

! javaIsInstalled && javaInstall
