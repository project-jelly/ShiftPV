package measurement

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type request struct {
	ID       string `json:"id"`
	Evidence string `json:"evidence"`
}

func newRequest(evidence string) (request, string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return request{}, "", err
	}
	r := request{ID: hex.EncodeToString(nonce[:]), Evidence: evidence}
	data, err := json.Marshal(r)
	return r, string(data), err
}

func parseRequest(encoded string) (request, error) {
	var r request
	if len(encoded) > 512 || json.Unmarshal([]byte(encoded), &r) != nil || !validHex(r.ID, 16) || !validHex(r.Evidence, 32) {
		return request{}, fmt.Errorf("invalid capacity probe request")
	}
	return r, nil
}

func validHex(value string, size int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == size && hex.EncodeToString(decoded) == value
}
