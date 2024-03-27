
################################################################################
#
# To get started, all you should need to do is source this script within
# ~/.bashrc and then fire up a new bash terminal or execute "source ~/.bashrc"
# in your current terminal
#
###############################################################################

# Resolve parent dir, ie 'bin'

DW_CLOUD_BIN="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Home is the parent of bin

DW_CLOUD_HOME="$( dirname "${DW_CLOUD_BIN}" )"

# Parent dir for all service-related data, just to simplify tear-down automation.
# Data directories for a given service may be overridden if desired, but in
# that case data cleanup is the responsibility of the service, via its
# '{serviceName}Uninstall' implementation

DW_CLOUD_DATA="${DW_CLOUD_HOME}/data"

# Parent dir for services

DW_CLOUD_PLUGINS="${DW_CLOUD_BIN}/services"

# Import common functions such as 'register', 'allInstall', 'allStart', etc

source "${DW_CLOUD_BIN}/common.sh"

############################ Service registration ########################################

# Here we use the 'register' function to create a simple registry of service names and to
# import (ie, 'source') any scripts required to configure/bootstrap each service
#
# The service contract requires the following:
#
# (1) The file "bin/services/{servicename}/bootstrap.sh" must exist
# (2) bootstrap.sh must implement the following wrapper functions...
#
# {servicename}Start       - Starts the service
# {servicename}Stop        - Stops the service
# {servicename}Status      - Displays current status of the service, including PIDs if running
# {servicename}Install     - Installs the service
# {servicename}Uninstall   - Uninstalls service, leaving binaries in place. Takes optional '--remove-binaries' flag
# {servicename}IsRunning   - Returns 0 if running, non-zero otherwise
# {servicename}IsInstalled - Returns 0 if installed, non-zero otherwise
# {servicename}Printenv    - Display current state of the service config
# {servicename}PidList     - Display all service PIDs on a single line, space-delimited

if jdkIsConfigured ; then
  register maven    # $DW_CLOUD_PLUGINS/maven/bootstrap.sh
  register hadoop   # $DW_CLOUD_PLUGINS/hadoop/bootstrap.sh
  register accumulo # $DW_CLOUD_PLUGINS/accumulo/bootstrap.sh
  register datawave # $DW_CLOUD_PLUGINS/datawave/bootstrap.sh
fi

# You may add/remove lines above to affect which services are activated in your environment

# Order of registration is important, as it reflects install and startup order within global wrapper
# functions such as 'allInstall', 'allStart', etc. Likewise, 'allStop' and 'allUninstall' perform
# actions in reverse order of registration. See bin/common.sh for more info


