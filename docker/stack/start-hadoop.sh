#!/usr/bin/env bash
set -euo pipefail

role="${1:?A Hadoop role is required}"

export JDK_JAVA_OPTIONS="--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/java.text=ALL-UNNAMED"

case "${role}" in
    namenode)
        if [[ ! -d /var/lib/hadoop-hdfs/name/current ]]; then
            hdfs namenode -format -force -nonInteractive
        fi
        exec hdfs namenode
        ;;
    datanode)
        exec hdfs datanode
        ;;
    resourcemanager)
        mapred --daemon start historyserver
        exec yarn resourcemanager
        ;;
    nodemanager)
        exec yarn nodemanager
        ;;
    *)
        echo "Unknown Hadoop role: ${role}" >&2
        exit 2
        ;;
esac
