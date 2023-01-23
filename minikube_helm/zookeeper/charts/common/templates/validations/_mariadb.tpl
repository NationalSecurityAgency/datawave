{{/* vim: set filetype=mustache: */}}
{{/*
Validate MariaDB required passwords are not empty.

Usage:
{{ include "common.validations.Values.zookeeper.mariadb.passwords" (dict "secret" "secretName" "subchart" false "context" $) }}
Params:
  - secret - String - Required. Name of the secret where MariaDB values are stored, e.g: "mysql-passwords-secret"
  - subchart - Boolean - Optional. Whether MariaDB is used as subchart or not. Default: false
*/}}
{{- define "common.validations.Values.zookeeper.mariadb.passwords" -}}
  {{- $existingSecret := include "common.mariadb.Values.zookeeper.auth.existingSecret" . -}}
  {{- $enabled := include "common.mariadb.Values.zookeeper.enabled" . -}}
  {{- $architecture := include "common.mariadb.Values.zookeeper.architecture" . -}}
  {{- $authPrefix := include "common.mariadb.Values.zookeeper.key.auth" . -}}
  {{- $valueKeyRootPassword := printf "%s.rootPassword" $authPrefix -}}
  {{- $valueKeyUsername := printf "%s.username" $authPrefix -}}
  {{- $valueKeyPassword := printf "%s.password" $authPrefix -}}
  {{- $valueKeyReplicationPassword := printf "%s.replicationPassword" $authPrefix -}}

  {{- if and (or (not $existingSecret) (eq $existingSecret "\"\"")) (eq $enabled "true") -}}
    {{- $requiredPasswords := list -}}

    {{- $requiredRootPassword := dict "valueKey" $valueKeyRootPassword "secret" .secret "field" "mariadb-root-password" -}}
    {{- $requiredPasswords = append $requiredPasswords $requiredRootPassword -}}

    {{- $valueUsername := include "common.utils.getValueFromKey" (dict "key" $valueKeyUsername "context" .context) }}
    {{- if not (empty $valueUsername) -}}
        {{- $requiredPassword := dict "valueKey" $valueKeyPassword "secret" .secret "field" "mariadb-password" -}}
        {{- $requiredPasswords = append $requiredPasswords $requiredPassword -}}
    {{- end -}}

    {{- if (eq $architecture "replication") -}}
        {{- $requiredReplicationPassword := dict "valueKey" $valueKeyReplicationPassword "secret" .secret "field" "mariadb-replication-password" -}}
        {{- $requiredPasswords = append $requiredPasswords $requiredReplicationPassword -}}
    {{- end -}}

    {{- include "common.validations.Values.zookeeper.multiple.empty" (dict "required" $requiredPasswords "context" .context) -}}

  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for existingSecret.

Usage:
{{ include "common.mariadb.Values.zookeeper.auth.existingSecret" (dict "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether MariaDB is used as subchart or not. Default: false
*/}}
{{- define "common.mariadb.Values.zookeeper.auth.existingSecret" -}}
  {{- if .subchart -}}
    {{- .context.Values.zookeeper.mariadb.auth.existingSecret | quote -}}
  {{- else -}}
    {{- .context.Values.zookeeper.auth.existingSecret | quote -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for enabled mariadb.

Usage:
{{ include "common.mariadb.Values.zookeeper.enabled" (dict "context" $) }}
*/}}
{{- define "common.mariadb.Values.zookeeper.enabled" -}}
  {{- if .subchart -}}
    {{- printf "%v" .context.Values.zookeeper.mariadb.enabled -}}
  {{- else -}}
    {{- printf "%v" (not .context.Values.zookeeper.enabled) -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for architecture

Usage:
{{ include "common.mariadb.Values.zookeeper.architecture" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether MariaDB is used as subchart or not. Default: false
*/}}
{{- define "common.mariadb.Values.zookeeper.architecture" -}}
  {{- if .subchart -}}
    {{- .context.Values.zookeeper.mariadb.architecture -}}
  {{- else -}}
    {{- .context.Values.zookeeper.architecture -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for the key auth

Usage:
{{ include "common.mariadb.Values.zookeeper.key.auth" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether MariaDB is used as subchart or not. Default: false
*/}}
{{- define "common.mariadb.Values.zookeeper.key.auth" -}}
  {{- if .subchart -}}
    mariadb.auth
  {{- else -}}
    auth
  {{- end -}}
{{- end -}}
