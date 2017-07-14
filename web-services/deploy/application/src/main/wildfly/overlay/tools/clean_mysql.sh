#!/bin/sh

MUSER="${1}"
MPASS="${2}"
MDB="${3}"
HOST="${4}"

# Detect paths
MYSQL=$(which mysql)
AWK=$(which awk)
GREP=$(which grep)

if [ $# -ne 4 ]; then
    echo "Usage: $0 {MySQL-username} {MySQL-password {MySQL-database} {MySQL-hostname}"
    echo "Drops all tables from a MySQL"
    exit 1
fi

TABLES=$($MYSQL -h $HOST -u $MUSER -p$MPASS $MDB -e 'show tables' | $AWK '{ print $1}' | $GREP -v '^Tables' )

echo $TABLES

for t in $TABLES
do
    if ((`expr "$t" : 'v*'` == 0)); then
        if [ "$t" != "template" -a "$t" != "cachedResultsQuery" ]; then
            echo "Deleting $t table from $MDB database..."
            $MYSQL -h $HOST -u $MUSER -p$MPASS $MDB -e "drop table $t"
        fi
    else
        echo "Deleting $t view from $MDB database..."
        $MYSQL -h $HOST -u $MUSER -p$MPASS $MDB -e "drop view $t"
    fi
done

echo "Deleting cachedResultsQuery table from $MDB database..."
$MYSQL -h $HOST -u $MUSER -p$MPASS $MDB -e "drop table cachedResultsQuery"

echo "Deleting template table from $MDB database..."
$MYSQL -h $HOST -u $MUSER -p$MPASS $MDB -e "drop table template"
