
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
# that case cleanup will be the responsibility of the service, via it's
# '{serviceName}Uninstall' implementation
DW_CLOUD_DATA="${DW_CLOUD_HOME}/data"

# Parent dir for services
DW_CLOUD_PLUGINS="${DW_CLOUD_BIN}/services"

# Import common functions
source "${DW_CLOUD_BIN}/common.sh"

####################### Begin service registration ###################################

# You may add/remove lines below to affect which services are available in your
# environment

# Services are made pluggable by following a few simple naming conventions for
# bootstrap scripts & function names. See the 'register' function in bin/common.sh
# for details
#
# Order of registration is also important, as it reflects install and startup order, i.e,
# for wrapper functions such as 'installAll', 'startAll', etc. Likewise, functions such
# as 'stopAll' and 'uninstallAll' will perform actions in reverse order of registration

register java     # $DW_CLOUD_PLUGINS/java/bootstrap.sh
register maven    # $DW_CLOUD_PLUGINS/maven/bootstrap.sh
register hadoop   # $DW_CLOUD_PLUGINS/hadoop/bootstrap.sh
register accumulo # $DW_CLOUD_PLUGINS/accumulo/bootstrap.sh
register datawave # $DW_CLOUD_PLUGINS/datawave/bootstrap.sh

####################### End service registration #######################################

# ! isLoginShell && isInteractiveShell && promptForInstallation
