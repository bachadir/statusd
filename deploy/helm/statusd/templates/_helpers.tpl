{{/* Where the catalogue is mounted (and what config.catalog_dir must point to). */}}
{{- define "statusd.catalogDir" -}}/etc/statusd/catalog{{- end -}}

{{/*
Catalogue mode:
  "files"    - the chart renders catalog.files into its own ConfigMap
  "existing" - catalog.existingConfigMap, managed outside the chart (CI, ArgoCD, kustomize)
  "image"    - neither set: the catalogue baked into the image at /etc/statusd/catalog
*/}}
{{- define "statusd.catalogMode" -}}
{{- if and .Values.catalog.files .Values.catalog.existingConfigMap -}}
{{- fail "catalog: set either catalog.files or catalog.existingConfigMap, not both" -}}
{{- else if .Values.catalog.files -}}files
{{- else if .Values.catalog.existingConfigMap -}}existing
{{- else -}}image
{{- end -}}
{{- end -}}

{{- define "statusd.catalogConfigMapName" -}}
{{- if eq (include "statusd.catalogMode" .) "existing" -}}
{{- .Values.catalog.existingConfigMap -}}
{{- else -}}
{{- .Release.Name }}-catalog
{{- end -}}
{{- end -}}

{{/*
Hash of the catalogue content, used as a pod annotation so a catalogue change rolls the pod.
For an external ConfigMap the live object is read with lookup (empty under `helm template`).
*/}}
{{- define "statusd.catalogChecksum" -}}
{{- $mode := include "statusd.catalogMode" . -}}
{{- if eq $mode "files" -}}
{{- toYaml .Values.catalog.files | sha256sum -}}
{{- else if eq $mode "existing" -}}
{{- $cm := lookup "v1" "ConfigMap" .Release.Namespace .Values.catalog.existingConfigMap -}}
{{- toYaml (default dict $cm.data) | sha256sum -}}
{{- else -}}
image-{{ .Values.image.tag }}
{{- end -}}
{{- end -}}

{{/* The statusd config with catalog_dir pinned to the mount path. */}}
{{- define "statusd.config" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $dir := include "statusd.catalogDir" . -}}
{{- with $cfg.catalog_dir -}}
{{- if ne . $dir -}}
{{- fail (printf "config.catalog_dir must be %s (or unset) in this chart; got %s" $dir .) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $cfg "catalog_dir" $dir -}}
{{- toYaml $cfg -}}
{{- end -}}

{{/* "true" for a raft witness (votes only: no catalogue, no public service). */}}
{{- define "statusd.isWitness" -}}
{{- if not (has .Values.role (list "node" "witness")) -}}
{{- fail (printf "role must be node or witness, got %q" .Values.role) -}}
{{- end -}}
{{- eq .Values.role "witness" -}}
{{- end -}}
