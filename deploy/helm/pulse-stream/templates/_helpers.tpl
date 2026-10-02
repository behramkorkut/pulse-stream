{{/*
Étiquettes qui IDENTIFIENT un groupe de pods : le Deployment s'en sert pour retrouver les siens, le Service pour
choisir vers quels pods envoyer le trafic. Elles ne doivent jamais changer après la première installation.
Appel : include "pulse.selectorLabels" (dict "root" $ "name" "collector")
*/}}
{{- define "pulse.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}

{{/* Étiquettes complètes : celles de sélection + de l'information pour les humains et les outils. */}}
{{- define "pulse.labels" -}}
{{ include "pulse.selectorLabels" . }}
app.kubernetes.io/part-of: pulse-stream
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end }}
