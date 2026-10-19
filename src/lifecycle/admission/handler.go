package admission

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
)

type Checker interface {
	Check(context.Context) (uninstallcheck.Report, error)
}

type Permit interface {
	Granted(context.Context) (bool, error)
}

type Handler struct {
	Checker           Checker
	Permit            Permit
	TrustedController string
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()
	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(request.Body).Decode(&review); err != nil {
		http.Error(writer, fmt.Sprintf("decode AdmissionReview: %v", err), http.StatusBadRequest)
		return
	}
	responseReview := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: admissionv1.SchemeGroupVersion.String(), Kind: "AdmissionReview"},
		Response: h.Admit(request.Context(), review.Request),
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(responseReview); err != nil {
		http.Error(writer, fmt.Sprintf("encode AdmissionReview: %v", err), http.StatusInternalServerError)
	}
}

func (h *Handler) Admit(ctx context.Context, request *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if request == nil {
		return denied("AdmissionReview has no request", "")
	}
	if isPoolUpdate(request) {
		protectedMetadataChanged, err := poolProtectedMetadataChanged(request)
		if err != nil {
			return denied(fmt.Sprintf("ShiftPV Pool update denied: %v", err), request.UID)
		}
		if !protectedMetadataChanged || (h.TrustedController != "" && request.UserInfo.Username == h.TrustedController) {
			return &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
		}
		return denied("ShiftPV Pool update denied: only the trusted controller may change lifecycle protection", request.UID)
	}
	if request.Operation != admissionv1.Delete {
		return &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
	}
	if trustedRuntimeDelete(request, h.TrustedController) {
		return &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
	}
	if h.Checker == nil || h.Permit == nil {
		return denied("ShiftPV uninstall protection is not configured", request.UID)
	}
	granted, err := h.Permit.Granted(ctx)
	if err != nil {
		return denied(fmt.Sprintf("ShiftPV resource deletion denied: read uninstall permit: %v", err), request.UID)
	}
	if granted {
		return &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
	}
	if isPoolDelete(request) {
		protected, err := poolDeletionProtected(request)
		if err != nil {
			return denied(fmt.Sprintf("ShiftPV Pool deletion denied: %v", err), request.UID)
		}
		if protected {
			return &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
		}
		return denied("ShiftPV Pool deletion denied: wait for the controller to install deletion protection", request.UID)
	}
	report, err := h.Checker.Check(ctx)
	if err != nil {
		return denied(fmt.Sprintf("ShiftPV resource deletion denied: inspect dependencies: %v", err), request.UID)
	}
	if report.Safe() {
		if isStorageClassDelete(request) {
			return &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
		}
		return denied("ShiftPV resource deletion denied: use the Helm or Argo CD uninstall guard to begin a quiesced teardown", request.UID)
	}

	return denied("ShiftPV resource deletion denied: dependent storage exists: "+blockerNames(report), request.UID)
}

func blockerNames(report uninstallcheck.Report) string {
	blockers := make([]string, 0, len(report.Blockers))
	for _, blocker := range report.Blockers {
		name := blocker.Name
		if blocker.Namespace != "" {
			name = blocker.Namespace + "/" + name
		}
		blockers = append(blockers, fmt.Sprintf("%s %s", blocker.Kind, name))
	}
	return strings.Join(blockers, ", ")
}

// isStorageClassDelete reports whether the request deletes a StorageClass. A StorageClass holds no
// data and the chart recreates it, so its deletion is allowed once the uninstall checker finds no
// dependent ShiftPV storage.
func isStorageClassDelete(request *admissionv1.AdmissionRequest) bool {
	return request.Resource.Group == "storage.k8s.io" && request.Resource.Resource == "storageclasses"
}

func isPoolDelete(request *admissionv1.AdmissionRequest) bool {
	return request.Operation == admissionv1.Delete && isPoolResource(request)
}

func isPoolUpdate(request *admissionv1.AdmissionRequest) bool {
	return request.Operation == admissionv1.Update && isPoolResource(request)
}

func isPoolResource(request *admissionv1.AdmissionRequest) bool {
	return request.Resource.Group == "shiftpv.io" && request.Resource.Resource == "shiftpvpools"
}

func poolDeletionProtected(request *admissionv1.AdmissionRequest) (bool, error) {
	if request.Name == "" {
		return false, fmt.Errorf("Pool name is missing")
	}
	var metadataOnly struct {
		Metadata struct {
			Finalizers []string `json:"finalizers"`
		} `json:"metadata"`
	}
	if len(request.OldObject.Raw) == 0 {
		return false, fmt.Errorf("Pool identity is missing")
	}
	if err := json.Unmarshal(request.OldObject.Raw, &metadataOnly); err != nil {
		return false, fmt.Errorf("read Pool identity: %w", err)
	}
	for _, finalizer := range metadataOnly.Metadata.Finalizers {
		if finalizer == uninstallcheck.PoolProtectionFinalizer {
			return true, nil
		}
	}
	return false, nil
}

func poolProtectedMetadataChanged(request *admissionv1.AdmissionRequest) (bool, error) {
	oldMetadata, err := objectMetadata(request.OldObject.Raw)
	if err != nil {
		return false, fmt.Errorf("read previous Pool lifecycle metadata: %w", err)
	}
	newMetadata, err := objectMetadata(request.Object.Raw)
	if err != nil {
		return false, fmt.Errorf("read updated Pool lifecycle metadata: %w", err)
	}
	requested := newMetadata.Annotations[volumeapi.PoolRetirementRequestAnnotation]
	if requested != "" && requested != oldMetadata.Annotations[volumeapi.PoolRetirementRequestAnnotation] {
		var previous struct {
			Metadata struct{ UID string } `json:"metadata"`
			Status   volumeapi.PoolStatus `json:"status"`
		}
		if err := json.Unmarshal(request.OldObject.Raw, &previous); err != nil {
			return false, err
		}
		if requested != previous.Metadata.UID || !(volumeapi.Pool{Status: previous.Status}).RegistrationApproved() {
			return false, fmt.Errorf("retirement request must name the UID of an approved registration")
		}
	}
	finalizerRemoved := slices.Contains(oldMetadata.Finalizers, uninstallcheck.PoolProtectionFinalizer) && !slices.Contains(newMetadata.Finalizers, uninstallcheck.PoolProtectionFinalizer)
	approvalChanged := oldMetadata.Annotations[uninstallcheck.PoolIdentityReleaseAnnotation] != newMetadata.Annotations[uninstallcheck.PoolIdentityReleaseAnnotation]
	retirementChanged := oldMetadata.Annotations[volumeapi.PoolRetirementApprovalAnnotation] != newMetadata.Annotations[volumeapi.PoolRetirementApprovalAnnotation]
	return finalizerRemoved || approvalChanged || retirementChanged, nil
}

type poolMetadata struct {
	Finalizers  []string
	Annotations map[string]string
}

func objectMetadata(raw []byte) (poolMetadata, error) {
	if len(raw) == 0 {
		return poolMetadata{}, fmt.Errorf("Pool object is missing")
	}
	var metadataOnly struct {
		Metadata struct {
			Finalizers  []string          `json:"finalizers"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &metadataOnly); err != nil {
		return poolMetadata{}, err
	}
	return poolMetadata{Finalizers: metadataOnly.Metadata.Finalizers, Annotations: metadataOnly.Metadata.Annotations}, nil
}

func trustedRuntimeDelete(request *admissionv1.AdmissionRequest, trustedUsername string) bool {
	if trustedUsername == "" || request.UserInfo.Username != trustedUsername || request.Resource.Group != "shiftpv.io" {
		return false
	}
	switch request.Resource.Resource {
	case "shiftpvvolumes", "shiftpvmoves":
		return true
	default:
		return false
	}
}

func denied(message string, uid types.UID) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		UID:     uid,
		Allowed: false,
		Result:  &metav1.Status{Status: metav1.StatusFailure, Message: message, Reason: metav1.StatusReasonForbidden, Code: http.StatusForbidden},
	}
}
