#!/bin/bash

GITHUB_TOKEN=$(gh auth token)
REPOSITORIES=(datawave datawave-authorization-service datawave-in-memory-accumulo datawave-accumulo-utils datawave-common-utils datawave-base-rest-responses datawave-type-utils datawave-metadata-utils datawave-accumulo-service datawave-dictionary-service datawave-audit-service datawave-query-metric-service datawave-metrics-reporter datawave-spring-boot-starter-cache datawave-spring-boot-starter datawave-hazelcast-service datawave-spring-boot-starter-metadata datawave-spring-boot-starter-query-metric datawave-spring-boot-starter-audit datawave-config-service datawave-parent datawave-service-parent datawave-utils datawave-spring-boot-starter-query datawave-query-service datawave-query-executor-service datawave-modification-service datawave-mapreduce-query-service datawave-spring-boot-starter-cached-results datawave-map-service datawave-file-provider-service ) # List your repositories here
ORG="NationalSecurityAgency"

function usage() {
    echo "Usage: $0 <username> <add|remove>"
    exit 1
}

function user_has_access() {
    local user=$1
    local repo=$2
    curl -s -H "Authorization: token $GITHUB_TOKEN" \
        "https://api.github.com/repos/$ORG/$repo/collaborators/$user/permission" | jq -r '.permission'
}

function add_user() {
    local user=$1
    local repo=$2
    curl -s -X PUT -H "Authorization: token $GITHUB_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"permission": "push"}' \
        "https://api.github.com/repos/$ORG/$repo/collaborators/$user"
}

function remove_user() {
    local user=$1
    local repo=$2
    curl -s -X DELETE -H "Authorization: token $GITHUB_TOKEN" \
        "https://api.github.com/repos/$ORG/$repo/collaborators/$user"
}

if [ "$#" -ne 2 ]; then
    usage
fi

USERNAME=$1
COMMAND=$2

for REPO in "${REPOSITORIES[@]}"; do
    PERMISSION=$(user_has_access $USERNAME $REPO)
    if [ "$COMMAND" == "add" ]; then
        if [ "$PERMISSION" != "write" ] && [ "$PERMISSION" != "admin" ]; then
            echo "Adding $USERNAME to $REPO"
            add_user $USERNAME $REPO
        else
            echo "$USERNAME already has write access to $REPO"
        fi
    elif [ "$COMMAND" == "remove" ]; then
        if [ "$PERMISSION" == "write" ] || [ "$PERMISSION" == "admin" ]; then
            echo "Removing $USERNAME from $REPO"
            remove_user $USERNAME $REPO
        else
            echo "$USERNAME does not have write access to $REPO"
        fi
    else
        usage
    fi
done

echo "Operation completed."

