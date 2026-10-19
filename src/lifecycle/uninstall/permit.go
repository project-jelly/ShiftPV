package uninstall

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	"github.com/project-jelly/ShiftPV/src/webhook/certificate"
)

const (
	permitLabel       = "shiftpv.io/uninstall-permit"
	permitValue       = "managed"
	stateQuiescing    = "quiescing"
	stateGranted      = "granted"
	permitTTL         = 5 * time.Minute
	stateKey          = "state"
	attemptKey        = "attempt"
	expiresAtKey      = "expiresAt"
	controllerAckKey  = "controllerAck"
	controllerAckDone = "observed"
)

// The uninstall guard only deletes lifecycle admission resources that the
// certificate controller produced, so the label vocabulary is borrowed from the
// producer instead of being restated here.
const (
	managedNameLabel      = certificate.NameLabel
	managedByLabel        = certificate.ManagedByLabel
	managedComponentLabel = certificate.ComponentLabel
)

type PermitStore struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
	CSIDriver string
	Now       func() time.Time
}

func (p *PermitStore) BeginQuiesce(ctx context.Context) (string, error) {
	if err := p.validate(); err != nil {
		return "", err
	}
	driver, err := p.currentDriver(ctx)
	if err != nil {
		return "", err
	}
	attempt, err := randomAttempt()
	if err != nil {
		return "", err
	}
	desired := p.desired(driver, stateQuiescing, attempt)
	configMaps := p.Client.CoreV1().ConfigMaps(p.Namespace)
	existing, err := configMaps.Get(ctx, p.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := configMaps.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return "", fmt.Errorf("create uninstall quiesce state: %w", err)
		}
		return attempt, nil
	}
	if err != nil {
		return "", fmt.Errorf("read uninstall quiesce state: %w", err)
	}
	if existing.Labels[permitLabel] != permitValue || !permitOwnedBy(existing, p.CSIDriver, driver.UID) {
		return "", fmt.Errorf("refuse to replace unmanaged uninstall state %s/%s", p.Namespace, p.Name)
	}
	desired.ResourceVersion = existing.ResourceVersion
	if _, err := configMaps.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return "", fmt.Errorf("update uninstall quiesce state: %w", err)
	}
	return attempt, nil
}

func (p *PermitStore) WaitForQuiesced(ctx context.Context, attempt string) error {
	if attempt == "" {
		return fmt.Errorf("uninstall attempt is required")
	}
	return wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		state, err := p.readState(ctx)
		if err != nil {
			return false, err
		}
		if state == nil || state.Data[attemptKey] != attempt || state.Data[stateKey] != stateQuiescing {
			return false, fmt.Errorf("uninstall quiesce state changed before controller acknowledgement")
		}
		return state.Data[controllerAckKey] == controllerAckDone, nil
	})
}

func (p *PermitStore) Quiescing(ctx context.Context) (string, bool, error) {
	state, err := p.readState(ctx)
	if err != nil || state == nil {
		return "", false, err
	}
	phase := state.Data[stateKey]
	return state.Data[attemptKey], phase == stateQuiescing || phase == stateGranted, nil
}

func (p *PermitStore) Acknowledge(ctx context.Context, attempt string) error {
	if attempt == "" {
		return fmt.Errorf("uninstall attempt is required")
	}
	configMaps := p.Client.CoreV1().ConfigMaps(p.Namespace)
	return retryUpdate(ctx, func() error {
		state, err := p.readState(ctx)
		if err != nil {
			return err
		}
		if state == nil || state.Data[stateKey] != stateQuiescing || state.Data[attemptKey] != attempt || state.Data[controllerAckKey] == controllerAckDone {
			return nil
		}
		state.Data[controllerAckKey] = controllerAckDone
		_, err = configMaps.Update(ctx, state, metav1.UpdateOptions{})
		return err
	})
}

func (p *PermitStore) Grant(ctx context.Context, attempt string) error {
	if attempt == "" {
		return fmt.Errorf("uninstall attempt is required")
	}
	configMaps := p.Client.CoreV1().ConfigMaps(p.Namespace)
	return retryUpdate(ctx, func() error {
		state, err := p.readState(ctx)
		if err != nil {
			return err
		}
		if state == nil || state.Data[stateKey] != stateQuiescing || state.Data[attemptKey] != attempt || state.Data[controllerAckKey] != controllerAckDone {
			return fmt.Errorf("controller has not acknowledged uninstall quiesce attempt %q", attempt)
		}
		state.Data[stateKey] = stateGranted
		_, err = configMaps.Update(ctx, state, metav1.UpdateOptions{})
		return err
	})
}

func (p *PermitStore) Granted(ctx context.Context) (bool, error) {
	state, err := p.readState(ctx)
	if err != nil || state == nil {
		return false, err
	}
	return state.Data[stateKey] == stateGranted && state.Data[controllerAckKey] == controllerAckDone, nil
}

func (p *PermitStore) Cancel(ctx context.Context, attempt string) error {
	state, err := p.readState(ctx)
	if err != nil || state == nil {
		return err
	}
	if state.Data[attemptKey] != attempt {
		return nil
	}
	uid, revision := state.UID, state.ResourceVersion
	err = p.Client.CoreV1().ConfigMaps(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &revision}})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cancel uninstall quiesce state: %w", err)
	}
	return nil
}

func (p *PermitStore) DisableValidation(ctx context.Context, configurationName string) error {
	if err := p.validate(); err != nil {
		return err
	}
	if configurationName == "" {
		return fmt.Errorf("lifecycle validation configuration name is required")
	}
	driver, err := p.currentDriver(ctx)
	if err != nil {
		return err
	}
	webhooks := p.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	configuration, err := webhooks.Get(ctx, configurationName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read lifecycle validation configuration: %w", err)
	}
	labels := configuration.GetLabels()
	if labels[managedNameLabel] != certificate.NameValue || labels[managedByLabel] != certificate.ManagedByValue || labels[managedComponentLabel] != certificate.ValidationComponent || !permitOwnedBy(configuration, p.CSIDriver, driver.UID) {
		return fmt.Errorf("refuse to delete unmanaged lifecycle validation configuration %q", configurationName)
	}
	uid := configuration.UID
	if err := webhooks.Delete(ctx, configurationName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete lifecycle validation configuration: %w", err)
	}
	return nil
}

func (p *PermitStore) readState(ctx context.Context) (*corev1.ConfigMap, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	state, err := p.Client.CoreV1().ConfigMaps(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read uninstall state: %w", err)
	}
	if state.Labels[permitLabel] != permitValue {
		return nil, fmt.Errorf("uninstall state %s/%s is unmanaged", p.Namespace, p.Name)
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, state.Data[expiresAtKey])
	if err != nil {
		return nil, fmt.Errorf("uninstall state %s/%s has invalid expiry", p.Namespace, p.Name)
	}
	if !p.now().Before(expiresAt) {
		return nil, nil
	}
	driver, err := p.currentDriver(ctx)
	if err != nil {
		return nil, err
	}
	if !permitOwnedBy(state, p.CSIDriver, driver.UID) {
		return nil, fmt.Errorf("uninstall state %s/%s has the wrong CSIDriver owner", p.Namespace, p.Name)
	}
	return state, nil
}

func (p *PermitStore) currentDriver(ctx context.Context) (*storagev1.CSIDriver, error) {
	driver, err := p.Client.StorageV1().CSIDrivers().Get(ctx, p.CSIDriver, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read uninstall state owner CSIDriver: %w", err)
	}
	return driver, nil
}

func (p *PermitStore) desired(driver *storagev1.CSIDriver, state, attempt string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
			Labels:    map[string]string{permitLabel: permitValue},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: storagev1.SchemeGroupVersion.String(), Kind: "CSIDriver", Name: driver.Name, UID: driver.UID,
			}},
		},
		Data: map[string]string{
			stateKey: state, attemptKey: attempt, expiresAtKey: p.now().Add(permitTTL).Format(time.RFC3339Nano),
		},
	}
}

func (p *PermitStore) validate() error {
	if p == nil || p.Client == nil || p.Namespace == "" || p.Name == "" || p.CSIDriver == "" {
		return fmt.Errorf("uninstall permit store is not configured")
	}
	return nil
}

func (p *PermitStore) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func permitOwnedBy(object metav1.Object, name string, uid types.UID) bool {
	for _, owner := range object.GetOwnerReferences() {
		if owner.APIVersion == storagev1.SchemeGroupVersion.String() && owner.Kind == "CSIDriver" && owner.Name == name && owner.UID == uid {
			return true
		}
	}
	return false
}

func randomAttempt() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate uninstall attempt: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func retryUpdate(ctx context.Context, update func() error) error {
	return wait.PollUntilContextCancel(ctx, 50*time.Millisecond, true, func(context.Context) (bool, error) {
		err := update()
		if apierrors.IsConflict(err) {
			return false, nil
		}
		return err == nil, err
	})
}
