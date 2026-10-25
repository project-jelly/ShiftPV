package registration

import (
	"encoding/json"
	"os"
	"testing"
)

// The API-server primitive test consumes the same corpus. CEL quantity's
// isInteger/asInteger use Quantity.AsInt64, including its fast-conversion limits.
func TestLimitValidationCorpus(t *testing.T) {
	data, err := os.ReadFile("testdata/limits.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Limit string `json:"limit"`
		Bytes int64  `json:"bytes"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Limit, func(t *testing.T) {
			got, err := ParseLimitBytes(tc.Limit)
			if got != tc.Bytes || (err == nil) != (tc.Bytes > 0) {
				t.Fatalf("bytes=%d err=%v want=%d", got, err, tc.Bytes)
			}
		})
	}
}
