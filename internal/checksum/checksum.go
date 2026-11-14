// Package checksum holds the sha256 helpers used for chunk and object integrity.
package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
)

// SHA256Hex returns the lowercase hex sha256 of data.
func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// New returns a streaming sha256 hasher.
func New() hash.Hash { return sha256.New() }

// Hex finalises a hasher into lowercase hex.
func Hex(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

// File hashes a file on disk without loading it into memory.
func File(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
