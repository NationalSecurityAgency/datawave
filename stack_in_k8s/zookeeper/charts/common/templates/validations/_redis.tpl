
{{/* vim: set filetype=mustache: */}}
{{/*
Validate Redis&reg; required passwords are not empty.

Usage:
{{ include "common.validations.Values.zookeeper.redis.passwords" (dict "secret" "secretName" "subchart" false "context" $) }}
Params:
  - secret - String - Required. Name of the secret where redis values are stored, e.g: "redis-passwords-secret"
  - subchart - Boolean - Optional. Whether redis is used as subchart or not. Default: false
*/}}
{{- define "common.validations.Values.zookeeper.redis.passwords" -}}
  {{- $enabled := include "common.redis.Values.zookeeper.enabled" . -}}
  {{- $valueKeyPrefix := include "common.redis.Values.zookeeper.keys.prefix" . -}}
  {{- $standarizedVersion := include "common.redis.Values.zookeeper.standarized.version" . }}

  {{- $existingSecret := ternary (printf "%s%s" $valueKeyPrefix "auth.existingSecret") (printf "%s%s" $valueKeyPrefix "existingSecret") (eq $standarizedVersion "true") }}
  {{- $existingSecretValue := include "common.utils.getValueFromKey" (dict "key" $existingSecret "context" .context) }}

  {{- $valueKeyRedisPassword := ternary (printf "%s%s" $valueKeyPrefix "auth.password") (printf "%s%s" $valueKeyPrefix "password") (eq $standarizedVersion "true") }}
  {{- $valueKeyRedisUseAuth := ternary (printf "%s%s" $valueKeyPrefix "auth.enabled") (printf "%s%s" $valueKeyPrefix "usePassword") (eq $standarizedVersion "true") }}

  {{- if and (or (not $existingSecret) (eq $existingSecret "\"\"")) (eq $enabled "true") -}}
    {{- $requiredPasswords := list -}}

    {{- $useAuth := include "common.utils.getValueFromKey" (dict "key" $valueKeyRedisUseAuth "context" .context) -}}
    {{- if eq $useAuth "true" -}}
      {{- $requiredRedisPassword := dict "valueKey" $valueKeyRedisPassword "secret" .secret "field" "redis-password" -}}
      {{- $requiredPasswords = append $requiredPasswords $requiredRedisPassword -}}
    {{- end -}}

    {{- include "common.validations.Values.zookeeper.multiple.empty" (dict "required" $requiredPasswords "context" .context) -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right value for enabled redis.

Usage:
{{ include "common.redis.Values.zookeeper.enabled" (dict "context" $) }}
*/}}
{{- define "common.redis.Values.zookeeper.enabled" -}}
  {{- if .subchart -}}
    {{- printf "%v" .context.Values.zookeeper.redis.enabled -}}
  {{- else -}}
    {{- printf "%v" (not .context.Values.zookeeper.enabled) -}}
  {{- end -}}
{{- end -}}

{{/*
Auxiliary function to get the right prefix path for the values

Usage:
{{ include "common.redis.Values.zookeeper.key.prefix" (dict "subchart" "true" "context" $) }}
Params:
  - subchart - Boolean - Optional. Whether redis is used as subchart or not. Default: false
*/}}
{{- define "common.redis.Values.zookeeper.keys.prefix" -}}
  {{- if .subchart -}}redis.{{- else -}}{{- end -}}
{{- end -}}

{{/*
Checks whether the redis chart's includes the standarizations (version >= 14)

Usage:
{{ include "common.redis.Values.zookeeper.standarized.version" (dict "context" $) }}
*/}}
{{- define "common.redis.Values.zookeeper.standarized.version" -}}

  {{- $standarizedAuth := printf "%s%s" (include "common.redis.Values.zookeeper.keys.prefix" .) "auth" -}}
  {{- $standarizedAuthValues := include "common.utils.getValueFromKey" (dict "key" $standarizedAuth "context" .context) }}

  {{- if $standarizedAuthValues -}}
    {{- true -}}
  {{- end -}}
{{- end -}}
