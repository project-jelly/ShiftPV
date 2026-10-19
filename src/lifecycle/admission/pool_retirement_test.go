package admission

import (
	"context"
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func retirementObject(request, approval string, approved bool) runtime.RawExtension {
	raw, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{"uid": "pool-uid", "finalizers": []string{volumeapi.PoolProtectionFinalizer},
			"annotations": map[string]string{volumeapi.PoolRetirementRequestAnnotation: request, volumeapi.PoolRetirementApprovalAnnotation: approval}},
		"status": map[string]any{"registrationApproved": approved},
	})
	return runtime.RawExtension{Raw: raw}
}

func TestPoolRetirementRequestAndApprovalAuthorization(t *testing.T) {
	trusted := "system:serviceaccount:system:controller"
	for _, test := range []struct {
		name, request, approval, caller string
		registered, allowed             bool
	}{
		{"operator requests current UID", "pool-uid", "", "operator", true, true},
		{"wrong request UID", "old-uid", "", "operator", true, false},
		{"unapproved registration", "pool-uid", "", "operator", false, false},
		{"operator forges approval", "", "pool-uid", "operator", true, false},
		{"controller records approval", "", "pool-uid", trusted, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := poolUpdateRequest(true, true)
			request.OldObject = retirementObject("", "", test.registered)
			request.Object = retirementObject(test.request, test.approval, test.registered)
			request.UserInfo.Username = test.caller
			if got := (&Handler{TrustedController: trusted}).Admit(context.Background(), request); got.Allowed != test.allowed {
				t.Fatalf("allowed=%v result=%+v", got.Allowed, got.Result)
			}
		})
	}
	request := poolUpdateRequest(true, true)
	request.OldObject, request.Object = retirementObject("pool-uid", "pool-uid", true), retirementObject("", "", true)
	if (&Handler{TrustedController: trusted}).Admit(context.Background(), request).Allowed {
		t.Fatal("operator cleared durable retirement approval")
	}
	request.Object = retirementObject("", "pool-uid", true)
	if !(&Handler{TrustedController: trusted}).Admit(context.Background(), request).Allowed {
		t.Fatal("removing a request must not erase or block retained approval")
	}
}
