package server

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"net/http"
	"sort"
	"strings"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

const v4SignatureSalt = "ots"

var canonicalHeaders = func() []string {
	names := []string{
		"x-ots-date",
		"x-ots-apiversion",
		"x-ots-accesskeyid",
		"x-ots-contentmd5",
		"x-ots-instancename",
		"x-ots-request-compress-size",
		"x-ots-response-compress-type",
		"x-ots-request-compress-type",
		"x-ots-ststoken",
		"x-ots-charge-for-admin",
		"x-ots-tunnel-type",
		"x-ots-playeraccountid",
		"x-ots-admin-task-type",
		"x-ots-admin-target-userid",
		"x-ots-sdk-traceid",
		"x-ots-request-priority",
		"x-ots-request-tag",
		"x-ots-request-search-tag",
		"x-ots-sourceip",
		"x-ots-issecuretransport",
		"x-ots-signregion",
		"x-ots-signdate",
	}
	sort.Strings(names)
	return names
}()

func canonicalStringToSign(r *http.Request, uri string) string {
	var builder strings.Builder
	builder.WriteString(uri)
	builder.WriteString("\nPOST\n\n")
	for _, name := range canonicalHeaders {
		value := r.Header.Get(name)
		if value == "" {
			continue
		}
		builder.WriteString(name)
		builder.WriteByte(':')
		builder.WriteString(strings.TrimSpace(value))
		builder.WriteByte('\n')
	}
	return builder.String()
}

func verifySignature(r *http.Request, uri string, secret string) error {
	stringToSign := canonicalStringToSign(r, uri)
	if v4 := r.Header.Get("x-ots-signaturev4"); v4 != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(stringToSign + v4SignatureSalt))
		return compareSignature(mac, v4)
	}
	if v1 := r.Header.Get("x-ots-signature"); v1 != "" {
		mac := hmac.New(sha1.New, []byte(secret))
		mac.Write([]byte(stringToSign))
		return compareSignature(mac, v1)
	}
	return ots.ErrAuthFailed("missing signature header")
}

func compareSignature(mac hash.Hash, provided string) error {
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(provided)) {
		return ots.ErrAuthFailed("signature mismatch")
	}
	return nil
}
