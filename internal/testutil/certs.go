// Package testutil holds helpers shared by tests. It is never imported by
// production code.
package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Certs are PEM file paths for a throwaway PKI: one CA, a server certificate
// valid for "localhost"/127.0.0.1, and a client certificate for mTLS.
type Certs struct {
	CA, ServerCert, ServerKey, ClientCert, ClientKey string
	// OtherCA is a second, unrelated CA (for negative tests).
	OtherCA string
}

// NewCerts writes a fresh PKI into t.TempDir().
func NewCerts(t testing.TB) Certs {
	t.Helper()
	dir := t.TempDir()
	caKey, caCert, caDER := newCA(t, "test-ca")
	_, _, otherDER := newCA(t, "other-ca")

	c := Certs{
		CA:         writePEM(t, dir, "ca.pem", "CERTIFICATE", caDER),
		OtherCA:    writePEM(t, dir, "other-ca.pem", "CERTIFICATE", otherDER),
		ServerCert: filepath.Join(dir, "server.pem"),
		ServerKey:  filepath.Join(dir, "server-key.pem"),
		ClientCert: filepath.Join(dir, "client.pem"),
		ClientKey:  filepath.Join(dir, "client-key.pem"),
	}
	issue(t, caCert, caKey, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "localhost"},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, c.ServerCert, c.ServerKey)
	issue(t, caCert, caKey, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "pipeline-client"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, c.ClientCert, c.ClientKey)
	return c
}

func newCA(t testing.TB, cn string) (*ecdsa.PrivateKey, *x509.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return key, cert, der
}

var serial int64 = 1

func issue(t testing.TB, ca *x509.Certificate, caKey *ecdsa.PrivateKey, tmpl *x509.Certificate, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial++
	tmpl.SerialNumber = big.NewInt(serial)
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(time.Hour)
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, filepath.Dir(certPath), filepath.Base(certPath), "CERTIFICATE", der)
	writePEM(t, filepath.Dir(keyPath), filepath.Base(keyPath), "EC PRIVATE KEY", keyDER)
}

func writePEM(t testing.TB, dir, name, typ string, der []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
