
DW_COLOR_RED="\033[31m"
DW_COLOR_GREEN="\033[32m"
DW_COLOR_YELLOW="\033[33m"
DW_COLOR_BLUE="\033[34m"
DW_COLOR_RESET="\033[m"

function info() {
   echo "[$( printGreen "DW-INFO" )] - $1"
}

function warn() {
   echo "[$( printYellow "DW-WARN" )] - $1"
}

function error() {
  echo "[$( printRed "DW-ERROR" )] - $1"
}

function fatal() {
   printRed "[DW-FATAL] - $1\n"
   printRed "Aborting $( basename "$0" )\n"
   exit 1
}

function printRed() {
    echo -ne "${DW_COLOR_RED}${1}${DW_COLOR_RESET}"
}

function printYellow() {
    echo -ne "${DW_COLOR_YELLOW}${1}${DW_COLOR_RESET}"
}

function printGreen() {
    echo -ne "${DW_COLOR_GREEN}${1}${DW_COLOR_RESET}"
}

function printBlue() {
    echo -ne "${DW_COLOR_BLUE}${1}${DW_COLOR_RESET}"
}