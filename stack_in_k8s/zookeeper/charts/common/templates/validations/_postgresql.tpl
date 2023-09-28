{{/* vim: set filetype=mustache: */}}
{{/*
Validate PostgreSQL required passwords are not empty.

Usage:
{{ include "common.validations.Values.zookeeper.postgresql.passwords" (dict "secret" "secretName" "subchart" false "context" $) }}
Params:
  - secret - String - Required. Name of the secret where postgresql values are stored, e.g: "postgresql-passwords-secret"
  - subchart - Boolean - Optional. Whether postgresql is used as subchart or not. Default: false
*/}}
{{- define "common.validations.Values.zookeeper.postgresql.passwords" -}}
  {{- $existingSecret := include "common.postgresql.Values.zookeeper.existingSecret" . -}}
  {{- $enabled := include "common.postgresql.Values.zookeeper.enabled" . -}}
  {{- $valueKeyPostgresqlPassword := include "common.postgresql.Values.zookeeper.key.postgressPassword" . -}}
  {{- $valueKeyPostgresqlReplicationEnabled := include "common.postgresql.Values.zookeeper.key.replicationPassword" . -}}
  {{- if and (or (not $existingSecret) (eq $existingSecret "\"\"")) (eq $enabled "true") -}}
    {{- $requiredPasswords := list -}}
    {{- $requiredPostgresqlPassword := dict "valueKey" $valueKeyPostgresqlPassword "secret" .secret "field" "postgresql-password" -}}
    {{- $requiredPasswords = append $requiredPasswords $requiredPostgresqlPassword -}}

    {{- $enabledReplication := include "common.postgresql.Values.zookeeper.enabled.replication" . -}}
    {{- if (eq $enabledReplication "true") -}}
        {{- $requiredPostgresqlReplicationPassword := dict "valueKey" $valueKeyPostgresqlReplicationEnabled "secret" .secret "field" "postgresql-replication-password" -}}
        {{- $requiredPasswords = append $requiredPasswords $requiredPostgresqlReplicationPassword -}}
    {{- end -}}

    {{- include "common.validations.Values.zookeeper.multiple.empty" (dict "required" $requiredPasswords "context" .context) -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to decide whether evaluate global values.

Usage:
{{ include "common.postgresql.Values.zookeeper.use.global" (dict "key" "key-of-global" "context" $) }}
Params:
  - key - String - Required. Field to be evaluated within global, e.g: "existingSecret"
*/}}
{{- define "common.postgresql.Values.zookeeper.use.global" -}}
  {{- if .context.Values.zookeeper.global -}}
    {{- if .context.Values.zookeeper.global.postgresql -}}
      {{- index .context.Values.zookeeper.global.postgresql .key | quote -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for existingSecret.

Usage:
{{ include "common.postgresql.Values.zookeeper.existingSecret" (dict "context" $) }}
*/}}
{{- define "common.postgresql.Values.zookeeper.existingSecret" -}}
  {{- $globalValue := include "common.postgresql.Values.zookeeper.use.global" (dict "key" "existingSecret" "context" .context) -}}

  {{- if .subchart -}}
    {{- default (.context.Values.zookeeper.postgresql.existingSecret | quote) $globalValue -}}
  {{- else -}}
    {{- default (.context.Values.zookeeper.existingSecret | quote) $globalValue -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for enabled postgresql.

Usage:
{{ include "common.postgresql.Values.zookeeper.enabled" (dict "context" $) }}
*/}}
{{- define "common.postgresql.Values.zookeeper.enabled" -}}
  {{- if .subchart -}}
    {{- printf "%v" .context.Values.zookeeper.postgresql.enabled -}}
  {{- else -}}
    {{- printf "%v" (not .context.Values.zookeeper.enabled) -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for the key postgressPassword.

Usage:
{{ include "common.postgresql.Values.zookeeper.key.postgressPassword" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether postgresql is used as subchart or not. Default: false
*/}}
{{- define "common.postgresql.Values.zookeeper.key.postgressPassword" -}}
  {{- $globalValue := include "common.postgresql.Values.zookeeper.use.global" (dict "key" "postgresqlUsername" "context" .context) -}}

  {{- if not $globalValue -}}
    {{- if .subchart -}}
      postgresql.postgresqlPassword
    {{- else -}}
      postgresqlPassword
    {{- end -}}
  {{- else -}}
    global.postgresql.postgresqlPassword
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for enabled.replication.

Usage:
{{ include "common.postgresql.Values.zookeeper.enabled.replication" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether postgresql is used as subchart or not. Default: false
*/}}
{{- define "common.postgresql.Values.zookeeper.enabled.replication" -}}
  {{- if .subchart -}}
    {{- printf "%v" .context.Values.zookeeper.postgresql.replication.enabled -}}
  {{- else -}}
    {{- printf "%v" .context.Values.zookeeper.replication.enabled -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for the key replication.password.

Usage:
{{ include "common.postgresql.Values.zookeeper.key.replicationPassword" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether postgresql is used as subchart or not. Default: false
*/}}
{{- define "common.postgresql.Values.zookeeper.key.replicationPassword" -}}
  {{- if .subchart -}}
    postgresql.replication.password
  {{- else -}}
    replication.password
  {{- end -}}
{{- end -}}
