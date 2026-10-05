//go:build linux || darwin

package ownership

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// ServingReceipt verifies the durable placement under the same per-volume lock
// used by create and reclaim, and hashes the exact device/inode/copy evidence.
func ServingReceipt(ctx context.Context, root string, identity volume.CopyIdentity, authority func(context.Context) error) (string, error) {
	store, err := Open(root, PoolIdentity{InstallationID: identity.InstallationID, PoolUID: identity.PoolUID})
	if err != nil {
		return "", err
	}
	defer store.Close()
	lock, err := store.Acquire(ctx, identity.VolumeID)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	if err := authority(ctx); err != nil {
		return "", err
	}
	if err := store.VerifyServing(identity); err != nil {
		return "", err
	}
	record, err := store.readPlacement(identity.CopyID)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
