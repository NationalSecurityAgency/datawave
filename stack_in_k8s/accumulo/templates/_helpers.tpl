{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "accumulo.name" -}}
{{- default .Chart.Name .Values.accumulo.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "accumulo.fullname" -}}
{{- if .Values.accumulo.fullnameOverride -}}
{{- .Values.accumulo.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.accumulo.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "accumulo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "accumulo.labels" -}}
helm.sh/chart: {{ include "accumulo.chart" . }}
{{ include "accumulo.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Values.accumulo.labels }}
{{ toYaml .Values.accumulo.labels }}
{{- end -}}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "accumulo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "accumulo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "accumulo.serviceAccountName" -}}
{{- if .Values.accumulo.serviceAccount.create -}}
    {{ default (include "accumulo.fullname" .) .Values.accumulo.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.accumulo.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- define "accumulo.zookeepers" -}}
    {{- required ".Values.accumulo.zookeeper.enabled = false, so .Values.accumulo.zookeeper.externalHosts must be set" .Values.accumulo.zookeeper.externalHosts }}
{{- end -}}

{{- define "accumulo.callSubChartTemplate" }}
{{- $dot := index . 0 }}
{{- $subchart := index . 1 | splitList "." }}
{{- $template := index . 2 }}
{{- $values := $dot.Values.accumulo }}
{{- range $subchart }}
{{- $values = index $values . }}
{{- end }}
{{- include $template (dict "Chart" (dict "Name" (last $subchart)) "Values" $values "Release" $dot.Release "Capabilities" $dot.Capabilities) }}
{{- end }}

{{- define "accumulo.hdfsNamenodeHostname" -}}
    {{ required ".Values.accumulo.hdfs.namenode.hostname needs to be set as .Values.accumulo.hdfs.enabled = false" .Values.accumulo.hdfs.namenode.hostname }}
  :{{ .Values.accumulo.hdfs.namenode.ports.clientRpc }}
{{- end -}}
