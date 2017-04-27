
function info() {
   echo "[DW-INFO] - $1"
}

function warn() {
   echo "[DW-WARN] - $1"
}

function error() {
   echo "[DW-ERROR] - $1"
}

function fatal() {
   echo "[DW-FATAL] - $1"
   echo "Aborting $( basename "$0" )" && exit 1
}
