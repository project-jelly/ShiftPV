{{- define "shiftpv.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "shiftpv.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- if contains (include "shiftpv.name" .) .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "shiftpv.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "shiftpv.baseLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "shiftpv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "shiftpv.labels" -}}
{{ include "shiftpv.baseLabels" . }}
shiftpv.io/uninstall-protected: "true"
{{- end }}

{{/*
Chart labels plus the component that owns the object.
Takes a dict of "root" (the chart context) and "component".
*/}}
{{- define "shiftpv.componentLabels" -}}
{{ include "shiftpv.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
The label set a workload's pods carry and every selector matches on.
Takes a dict of "root" and "component".
*/}}
{{- define "shiftpv.selectorLabels" -}}
app.kubernetes.io/name: {{ include "shiftpv.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "shiftpv.controllerImage" -}}
{{- printf "%s:%s" .Values.controller.image.repository .Values.controller.image.tag -}}
{{- end }}

{{- define "shiftpv.helperPodImage" -}}
{{- default (include "shiftpv.controllerImage" .) .Values.helperPod.image -}}
{{- end }}

{{- define "shiftpv.mobilityHelperImage" -}}
{{- default (include "shiftpv.controllerImage" .) .Values.mobility.helperImage -}}
{{- end }}

{{- define "shiftpv.placementPriorityClassName" -}}
{{- printf "%s-placement" (include "shiftpv.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "shiftpv.controllerServiceAccount" -}}
{{- default (printf "%s-controller" (include "shiftpv.fullname" .)) .Values.serviceAccount.controller.name }}
{{- end }}

{{- define "shiftpv.nodeServiceAccount" -}}
{{- default (printf "%s-node" (include "shiftpv.fullname" .)) .Values.serviceAccount.node.name }}
{{- end }}

{{- define "shiftpv.helperServiceAccount" -}}
{{- default (printf "%s-helper" (include "shiftpv.fullname" .)) .Values.serviceAccount.helper.name }}
{{- end }}

{{- define "shiftpv.uninstallGuardName" -}}
{{- printf "%s-uninstall-guard" (include "shiftpv.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "shiftpv.uninstallPermitName" -}}
{{- printf "%s-uninstall-permit" (include "shiftpv.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "shiftpv.webhookServiceName" -}}
{{- printf "%s-webhook" (include "shiftpv.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "shiftpv.webhookSecretName" -}}
{{- printf "%s-webhook-tls" (include "shiftpv.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "shiftpv.webhookConfigurationName" -}}
{{- printf "%s-mobility" (include "shiftpv.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "shiftpv.validationWebhookConfigurationName" -}}
{{- printf "%s-lifecycle" (include "shiftpv.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
The container security context every ShiftPV and CSI sidecar container uses.
Takes a dict of "runAsUser" and an optional "runAsNonRoot"; the node plugin
sidecars share the privileged plugin's uid instead of running as nonroot.
*/}}
{{- define "shiftpv.containerSecurityContext" -}}
{{- if not (hasKey . "runAsUser") -}}
{{- fail "containerSecurityContext needs runAsUser" -}}
{{- end -}}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
{{- if .runAsNonRoot }}
runAsNonRoot: true
{{- end }}
runAsUser: {{ get . "runAsUser" }}
{{- end }}

{{/*
The namespace of the running pod, which both controller containers read to
scope leader election and helper pods to the release namespace.
*/}}
{{- define "shiftpv.podNamespaceEnv" -}}
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
{{- end }}

{{/*
The CSI livenessprobe sidecar, which the controller and the node plugin both
run against their own socket. Takes a dict of "root", "logLevel",
"socketVolume", "socketDir", "runAsUser" and an optional "runAsNonRoot".
*/}}
{{- define "shiftpv.livenessProbeContainer" -}}
- name: liveness-probe
  image: {{ .root.Values.sidecars.livenessProbe.image }}
  args:
    - --csi-address={{ .socketDir }}/csi.sock
    - --health-port=9808
    - --v={{ .logLevel }}
  ports:
    - name: healthz
      containerPort: 9808
  livenessProbe:
    httpGet:
      path: /healthz
      port: healthz
    initialDelaySeconds: 10
    timeoutSeconds: 3
    periodSeconds: 10
  securityContext:
    {{- include "shiftpv.containerSecurityContext" . | nindent 4 }}
  volumeMounts:
    - name: {{ .socketVolume }}
      mountPath: {{ .socketDir }}
  {{- with .root.Values.sidecars.livenessProbe.resources }}
  resources:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}

{{/*
The body shared by every (Cluster)RoleBinding the chart ships. Takes a dict of
"root", "kind", "name" and "serviceAccount".
*/}}
{{- define "shiftpv.bindingBody" -}}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: {{ .kind }}
  name: {{ .name }}
subjects:
  - kind: ServiceAccount
    name: {{ .serviceAccount }}
    namespace: {{ .root.Release.Namespace }}
{{- end }}

{{/*
The scrape target Service for one component's metrics endpoint. Takes a dict of
"root" and "component".
*/}}
{{- define "shiftpv.metricsService" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "shiftpv.fullname" .root }}-{{ .component }}-metrics
  namespace: {{ .root.Release.Namespace }}
  labels:
    {{- include "shiftpv.componentLabels" . | nindent 4 }}
    shiftpv.io/metrics: "true"
spec:
  type: ClusterIP
  selector:
    {{- include "shiftpv.selectorLabels" . | nindent 4 }}
  ports:
    - name: metrics
      port: {{ .root.Values.metrics.port }}
      targetPort: metrics
{{- end }}
