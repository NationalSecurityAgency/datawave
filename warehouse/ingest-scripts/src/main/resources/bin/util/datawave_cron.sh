#!/bin/bash

# Source the environment so that we get all of the environment variables from the datawave RPM
. $HOME/.bash_profile

# Now run the command that was passed in
exec ${@:+"$@"}
