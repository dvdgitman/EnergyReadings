{{/*
Common labels applied to every resource
*/}}
{{- define "energy-pipeline.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
assignment-id: {{ .Values.assignmentId }}
{{- end }}
