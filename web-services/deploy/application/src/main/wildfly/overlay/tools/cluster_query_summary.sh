#!/bin/bash

usage() {
  echo "Usage: $0  [-u accumulo_user] [-p accumulo_password] [-d YYYY-MM-DD] [-s YYYY-MM-DD -e YYYY-MM-DD] [-o output_dir] [-n cluster_name] [-c]"
  echo "    REQUIRED: [-u accumulo_user] the accumulo user"
  echo "    REQUIRED: [-p accumulo_password] the accumulo password" 
  echo "    [-d YYYY-MM-DD] for a specific day (defaults to yesterday)"
  echo "    [-s YYYY-MM-DD -e YYYY-MM-DD] for start and end date range when searching multiple days."
  echo "    [-o output dir] the output directory (default to current dir)" 
  echo "    [-n cluster_name] The cluster name to be used in output reporting (defaults to empty string)" 
  echo "    [-c] Display the counts by day"
  echo ""
  exit
}

yesterday=`date -d yesterday +%Y-%m-%d`

SDAY="${yesterday}"
EDAY="${yesterday}"
AC_USER=""
AC_PASS=""
OUTPUT_DIR=${PWD}
CLUSTER_NAME=""
BYDAY=0

while getopts cs:u:d:o:p:e:n:h? o; do
  case $o in
   [ds]) SDAY=$OPTARG
     echo $SDAY | egrep '[0-9]{4}-[0-9]{2}-[0-9]{2}' 2>&1 > /dev/null
     if [ "$?" != "0" ]; then
        echo "DATE must be in YYYY-MM-DD format, not $SDAY"
        exit 1
     fi
     EDAY="${SDAY}"
     ;;
  e) EDAY=${OPTARG}
     echo $EDAY | egrep '[0-9]{4}-[0-9]{2}-[0-9]{2}' 2>&1 > /dev/null
     if [ "$?" != "0" ]; then
        echo "EDATE must be in YYYY-MM-DD format, not $EDAY"
        exit 1
     fi
    ;;
  u) AC_USER=${OPTARG}
    ;;
  p) AC_PASS=${OPTARG}
    ;;
  o) OUTPUT_DIR=${OPTARG}
   ;;
  n) CLUSTER_NAME=${OPTARG}
   ;;
  c) BYDAY=1
   ;;
  [?h*]) usage
  ;;
  esac
done


if [[ "${AC_USER}" == "" ||  "${AC_PASS}" == "" ]] ; then
 echo "Accumulo User/Password cannot be blank."
 usage
fi

########### Top 10 User Summary ####################

OUTPUT_FILE=${OUTPUT_DIR}/top_users_summary_${SDAY}_${EDAY}.txt
/opt/accumulo/current/bin/accumulo shell -u ${AC_USER} -p ${AC_PASS} -e "scan -t QueryAuditTable -b ${SDAY} -e ${EDAY}~ -np" | sed 's/T[0-9][0-9]:.*\[userDN://g; s/,.*, auditType:.*$//g' | sort | uniq -c | sort -nr |sort -nr > $OUTPUT_FILE
`chmod 664 ${OUTPUT_FILE}`

unique_total=`cat ${OUTPUT_FILE} | awk '{print $3}' | sort | uniq | wc -l`
unique_users=`grep -v "\." ${OUTPUT_FILE} | awk '{print $3}' | sort | uniq | wc -l`
unique_servers=`grep "\." ${OUTPUT_FILE} | awk '{print $3}' | sort | uniq | wc -l`
total_query_count=`awk '{SUM += $1} END {print SUM}' ${OUTPUT_FILE}`
total_user_query_count=`grep -v "\." ${OUTPUT_FILE} | awk '{SUM += $1} END {print SUM}'`
total_server_query_count=`grep "\." ${OUTPUT_FILE}| awk '{SUM += $1} END {print SUM}'`
if [[ "${BYDAY}" == "1" ]]; then
  unique_total_by_day=`cat ${OUTPUT_FILE} | awk '{print $2}' | sort | uniq -c | sort -k 2`
  unique_users_by_day=`grep -v "\." ${OUTPUT_FILE} | awk '{print $2}' | sort | uniq -c | sort -k 2`
  unique_servers_by_day=`grep "\." ${OUTPUT_FILE} | awk '{print $2}' | sort | uniq -c | sort -k 2`
  total_query_count_by_day=`cat ${OUTPUT_FILE} | sort -k 2 | awk 'BEGIN {u="START"; c=0;} {if ($2==u) c=c+$1; if ($2!=u) printf("%10d %s\n",c,u); if ($2!=u) c=$1; u=$2} END {printf("%10d %s\n",c,u);}' | grep -v START | sort -k 2`
  total_user_query_count_by_day=`grep -v "\." ${OUTPUT_FILE} | sort -k 2 | awk 'BEGIN {u="START"; c=0;} {if ($2==u) c=c+$1; if ($2!=u) printf("%10d %s\n",c,u); if ($2!=u) c=$1; u=$2} END {printf("%10d %s\n",c,u);}' | grep -v START | sort -k 2`
  total_server_query_count_by_day=`grep "\." ${OUTPUT_FILE} | sort -k 2 | awk 'BEGIN {u="START"; c=0;} {if ($2==u) c=c+$1; if ($2!=u) printf("%10d %s\n",c,u); if ($2!=u) c=$1; u=$2} END {printf("%10d %s\n",c,u);}' | grep -v START | sort -k 2`
  top_server_query_count=`grep "\." ${OUTPUT_FILE} | head -10 | awk '{print "   " $0}'`
  top_user_query_count=`grep -v "\." ${OUTPUT_FILE} | head -10 | awk '{print "   " $0}'`
else
  top_server_query_count=`grep "\." ${OUTPUT_FILE} | sort -k 3 | awk 'BEGIN {u="START"; c=0;} {if ($NF==u) c=c+$1; if ($NF!=u) printf("%10d %s\n",c,u); if ($NF!=u) c=$1; u=$NF} END {printf("%10d %s\n",c,u);}' | grep -v START | sort -nr | head -10`
  top_user_query_count=`grep -v "\." ${OUTPUT_FILE} | sort -k 3 | awk 'BEGIN {u="START"; c=0;} {if ($NF==u) c=c+$1; if ($NF!=u) printf("%10d %s\n",c,u); if ($NF!=u) c=$1; u=$NF} END {printf("%10d %s\n",c,u);}' | grep -v START | sort -nr | head -10`
fi

echo ""
echo "${CLUSTER_NAME} User Counts for Date Range: ${SDAY} - ${EDAY}"
if [[ "${BYDAY}" == "1" ]]; then
  echo "Unique ${CLUSTER_NAME} Total Users and Servers: $unique_total"
  echo -e "Unique ${CLUSTER_NAME} Users and Servers By Day:\n$unique_total_by_day"
  echo "Unique ${CLUSTER_NAME} Users: $unique_users"
  echo -e "Unique ${CLUSTER_NAME} Users By Day:\n$unique_users_by_day"
  echo "Unique ${CLUSTER_NAME} Servers: $unique_servers"
  echo -e "Unique ${CLUSTER_NAME} Servers By Day:\n$unique_servers_by_day"
  echo "Total ${CLUSTER_NAME} Queries: ${total_query_count}"
  echo -e "Total ${CLUSTER_NAME} Queries By Day:\n${total_query_count_by_day}"
  echo "Total ${CLUSTER_NAME} User Queries: ${total_user_query_count}"
  echo -e "Total ${CLUSTER_NAME} User Queries By Day:\n${total_user_query_count_by_day}"
  echo "Total ${CLUSTER_NAME} Server Queries: ${total_server_query_count}"
  echo -e "Total ${CLUSTER_NAME} Server Queries By Day:\n${total_server_query_count_by_day}"
else
  echo "Unique ${CLUSTER_NAME} Total Users and Servers: $unique_total"
  echo "Unique ${CLUSTER_NAME} Users: $unique_users"
  echo "Unique ${CLUSTER_NAME} Servers: $unique_servers"
  echo "Total ${CLUSTER_NAME} Queries: ${total_query_count}"
  echo "Total ${CLUSTER_NAME} User Queries: ${total_user_query_count}"
  echo "Total ${CLUSTER_NAME} Server Queries: ${total_server_query_count}"
fi
echo -e "Top 10 ${CLUSTER_NAME} User Queries Count:\n${top_user_query_count}"
echo -e "Top 10 ${CLUSTER_NAME} Server Queries Count:\n${top_server_query_count}"
echo ""

########### System From Summary ####################

OUTPUT_FILE=${OUTPUT_DIR}/system_from_summary_${SDAY}_${EDAY}.txt
/opt/accumulo/current/bin/accumulo shell -u ${AC_USER} -p ${AC_PASS} -e "scan -t QueryAuditTable -b ${SDAY} -e ${EDAY}~ -np" | sed 's/T[0-9][0-9]:.* systemFrom://g; s/, purpose:.*$//g' | sort | uniq -c | sort -nr |sort -nr > $OUTPUT_FILE
`chmod 664 ${OUTPUT_FILE}`
unique_system_from=`cat ${OUTPUT_FILE} | awk '{print $3}' | sort | uniq | wc -l`
if [[ "${BYDAY}" == "1" ]]; then
  system_from_summary=`cat ${OUTPUT_FILE} | awk '{print "   " $0}'`
else
  system_from_summary=`cat ${OUTPUT_FILE} | sort -k 3 | awk 'BEGIN {u="START"; c=0;} {if ($3==u) c=c+$1; if ($3!=u) printf("%10d %s\n",c,u); if ($3!=u) c=$1; u=$3} END {printf("%10d %s\n",c,u);}' | grep -v START | sort -nr`
fi

echo ""
echo "${CLUSTER_NAME} System From Counts for Date Range: ${SDAY} - ${EDAY}"
echo "Unique ${CLUSTER_NAME} Systems: $unique_system_from"
echo -e "${CLUSTER_NAME} System From Details:\n${system_from_summary}"
echo ""

########### Query Logic Summary ####################

OUTPUT_FILE=${OUTPUT_DIR}/query_logic_summary_${SDAY}_${EDAY}.txt
/opt/accumulo/current/bin/accumulo shell -u ${AC_USER} -p ${AC_PASS} -e "scan -t QueryAuditTable -b ${SDAY} -e ${EDAY}~ -np" | sed 's/T[0-9][0-9]:.* logicClass://g; s/, auditType:.*$//g' | sort | uniq -c | sort -nr |sort -nr > $OUTPUT_FILE
`chmod 664 ${OUTPUT_FILE}`
unique_query_logics=`cat ${OUTPUT_FILE} | awk '{print $3}' | sort | uniq | wc -l`
if [[ "${BYDAY}" == "1" ]]; then
  query_logic_summary=`cat ${OUTPUT_FILE} | awk '{print "   " $0}'`
else
  query_logic_summary=`cat ${OUTPUT_FILE} | sort -k 3 | awk 'BEGIN {u="START"; c=0;} {if ($3==u) c=c+$1; if ($3!=u) printf("%10d %s\n",c,u); if ($3!=u) c=$1; u=$3} END {printf("%10d %s\n",c,u);}' | grep -v START | sort -nr`
fi

echo ""
echo "${CLUSTER_NAME} Query Logic Counts for Date Range: ${SDAY} - ${EDAY}"
echo "Unique ${CLUSTER_NAME} Query Logics: $unique_query_logics"
echo -e "${CLUSTER_NAME} Query Logic Details:\n${query_logic_summary}"
echo ""

