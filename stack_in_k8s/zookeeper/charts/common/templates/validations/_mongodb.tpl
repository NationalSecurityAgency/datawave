{{/* vim: set filetype=mustache: */}}
{{/*
Validate MongoDB&reg; required passwords are not empty.

Usage:
{{ include "common.validations.Values.zookeeper.mongodb.passwords" (dict "secret" "secretName" "subchart" false "context" $) }}
Params:
  - secret - String - Required. Name of the secret where MongoDB&reg; values are stored, e.g: "mongodb-passwords-secret"
  - subchart - Boolean - Optional. Whether MongoDB&reg; is used as subchart or not. Default: false
*/}}
{{- define "common.validations.Values.zookeeper.mongodb.passwords" -}}
  {{- $existingSecret := include "common.mongodb.Values.zookeeper.auth.existingSecret" . -}}
  {{- $enabled := include "common.mongodb.Values.zookeeper.enabled" . -}}
  {{- $authPrefix := include "common.mongodb.Values.zookeeper.key.auth" . -}}
  {{- $architecture := include "common.mongodb.Values.zookeeper.architecture" . -}}
  {{- $valueKeyRootPassword := printf "%s.rootPassword" $authPrefix -}}
  {{- $valueKeyUsername := printf "%s.username" $authPrefix -}}
  {{- $valueKeyDatabase := printf "%s.database" $authPrefix -}}
  {{- $valueKeyPassword := printf "%s.password" $authPrefix -}}
  {{- $valueKeyReplicaSetKey := printf "%s.replicaSetKey" $authPrefix -}}
  {{- $valueKeyAuthEnabled := printf "%s.enabled" $authPrefix -}}

  {{- $authEnabled := include "common.utils.getValueFromKey" (dict "key" $valueKeyAuthEnabled "context" .context) -}}

  {{- if and (or (not $existingSecret) (eq $existingSecret "\"\"")) (eq $enabled "true") (eq $authEnabled "true") -}}
    {{- $requiredPasswords := list -}}

    {{- $requiredRootPassword := dict "valueKey" $valueKeyRootPassword "secret" .secret "field" "mongodb-root-password" -}}
    {{- $requiredPasswords = append $requiredPasswords $requiredRootPassword -}}

    {{- $valueUsername := include "common.utils.getValueFromKey" (dict "key" $valueKeyUsername "context" .context) }}
    {{- $valueDatabase := include "common.utils.getValueFromKey" (dict "key" $valueKeyDatabase "context" .context) }}
    {{- if and $valueUsername $valueDatabase -}}
        {{- $requiredPassword := dict "valueKey" $valueKeyPassword "secret" .secret "field" "mongodb-password" -}}
        {{- $requiredPasswords = append $requiredPasswords $requiredPassword -}}
    {{- end -}}

    {{- if (eq $architecture "replicaset") -}}
        {{- $requiredReplicaSetKey := dict "valueKey" $valueKeyReplicaSetKey "secret" .secret "field" "mongodb-replica-set-key" -}}
        {{- $requiredPasswords = append $requiredPasswords $requiredReplicaSetKey -}}
    {{- end -}}

    {{- include "common.validations.Values.zookeeper.multiple.empty" (dict "required" $requiredPasswords "context" .context) -}}

  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for existingSecret.

Usage:
{{ include "common.mongodb.Values.zookeeper.auth.existingSecret" (dict "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether MongoDb is used as subchart or not. Default: false
*/}}
{{- define "common.mongodb.Values.zookeeper.auth.existingSecret" -}}
  {{- if .subchart -}}
    {{- .context.Values.zookeeper.mongodb.auth.existingSecret | quote -}}
  {{- else -}}
    {{- .context.Values.zookeeper.auth.existingSecret | quote -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for enabled mongodb.

Usage:
{{ include "common.mongodb.Values.zookeeper.enabled" (dict "context" $) }}
*/}}
{{- define "common.mongodb.Values.zookeeper.enabled" -}}
  {{- if .subchart -}}
    {{- printf "%v" .context.Values.zookeeper.mongodb.enabled -}}
  {{- else -}}
    {{- printf "%v" (not .context.Values.zookeeper.enabled) -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for the key auth

Usage:
{{ include "common.mongodb.Values.zookeeper.key.auth" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether MongoDB&reg; is used as subchart or not. Default: false
*/}}
{{- define "common.mongodb.Values.zookeeper.key.auth" -}}
  {{- if .subchart -}}
    mongodb.auth
  {{- else -}}
    auth
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for architecture

Usage:
{{ include "common.mongodb.Values.zookeeper.architecture" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether MongoDB&reg; is used as subchart or not. Default: false
*/}}
{{- define "common.mongodb.Values.zookeeper.architecture" -}}
  {{- if .subchart -}}
    {{- .context.Values.zookeeper.mongodb.architecture -}}
  {{- else -}}
    {{- .context.Values.zookeeper.architecture -}}
  {{- end -}}
{{- end -}}
