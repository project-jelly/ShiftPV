package volume

import (
	"fmt"
	"strings"
)

// DriverName is the one CSI driver name this installation answers to. It is
// declared here, with the pure identity rules, so the CSI adapter, the
// admission boundary and the uninstall guard cannot drift apart.
const DriverName = "csi.shiftpv.io"

const CreationCancellationPrefix = "cancel-create-"

// IsCreationCancellation identifies the original initial serving copy and its
// cancellation operation. Callers must separately verify live authority.
func IsCreationCancellation(copy CopyIdentity, operationID string) bool {
	return copy.Validate() == nil && copy.Role == RoleServing && copy.CopyID == "initial-"+copy.VolumeUID &&
		ValidIdentityToken(operationID) && operationID == CreationCancellationPrefix+copy.VolumeUID
}

const (
	RoleServing  = "Serving"
	RoleIncoming = "Incoming"
	RoleRetired  = "Retired"
)

// CopyIdentity is the complete identity of one data-bearing directory. Names
// locate API objects; UIDs prevent a recreated object from inheriting authority.
type CopyIdentity struct {
	InstallationID string `json:"installationID"`
	PoolName       string `json:"poolName"`
	PoolUID        string `json:"poolUID"`
	VolumeID       string `json:"volumeID"`
	VolumeUID      string `json:"volumeUID"`
	CopyID         string `json:"copyID"`
	NodeName       string `json:"nodeName"`
	Role           string `json:"role"`
}

func (identity CopyIdentity) Validate() error {
	if !ValidIdentityToken(identity.InstallationID) || !ValidObjectName(identity.PoolName) ||
		!ValidIdentityToken(identity.PoolUID) || ValidateID(identity.VolumeID) != nil ||
		!ValidIdentityToken(identity.VolumeUID) || !ValidIdentityToken(identity.CopyID) ||
		!ValidObjectName(identity.NodeName) {
		return fmt.Errorf("invalid copy identity")
	}
	if identity.Role != RoleServing && identity.Role != RoleIncoming && identity.Role != RoleRetired {
		return fmt.Errorf("invalid copy role %q", identity.Role)
	}
	return nil
}

func ValidIdentityToken(value string) bool {
	return value != "" && len(value) <= 128 && value != "." && value != ".." && !strings.ContainsAny(value, "/\\\x00")
}

func ValidObjectName(value string) bool {
	return ValidIdentityToken(value) && len(value) <= 253
}
