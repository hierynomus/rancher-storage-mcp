{{/*
Expand the name of the chart.
*/}}
{{- define "rancher-storage-mcp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "rancher-storage-mcp.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label.
*/}}
{{- define "rancher-storage-mcp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "rancher-storage-mcp.labels" -}}
helm.sh/chart: {{ include "rancher-storage-mcp.chart" . }}
{{ include "rancher-storage-mcp.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "rancher-storage-mcp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rancher-storage-mcp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Name of the Secret that holds Rancher credentials.
*/}}
{{- define "rancher-storage-mcp.rancherSecretName" -}}
{{- if .Values.rancher.existingSecret -}}
{{- .Values.rancher.existingSecret -}}
{{- else -}}
{{- include "rancher-storage-mcp.fullname" . }}-rancher
{{- end -}}
{{- end }}

{{/*
TLS secret name for Ingress.
*/}}
{{- define "rancher-storage-mcp.tlsSecretName" -}}
{{- if .Values.ingress.tls.secretName }}
{{- .Values.ingress.tls.secretName }}
{{- else }}
{{- printf "%s-tls" (include "rancher-storage-mcp.fullname" .) }}
{{- end }}
{{- end }}
