package tlsutil

import (
	"crypto/tls"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/testutil"
)

func TestDisabledReturnsNil(t *testing.T) {
	c, err := Config{}.Client()
	if c != nil || err != nil {
		t.Fatalf("client: %v %v", c, err)
	}
	s, err := Config{}.Server()
	if s != nil || err != nil {
		t.Fatalf("server: %v %v", s, err)
	}
}

func TestConfigErrors(t *testing.T) {
	certs := testutil.NewCerts(t)
	junk := filepath.Join(t.TempDir(), "junk.pem")
	os.WriteFile(junk, []byte("not a cert"), 0o600)

	bad := map[string]func() error{
		"server without cert": func() error { _, err := Config{Enabled: true}.Server(); return err },
		"server missing file": func() error {
			_, err := Config{Enabled: true, CertFile: "/nope", KeyFile: "/nope"}.Server()
			return err
		},
		"server bad CA": func() error {
			_, err := Config{Enabled: true, CertFile: certs.ServerCert, KeyFile: certs.ServerKey, CAFile: junk}.Server()
			return err
		},
		"client missing CA": func() error { _, err := Config{Enabled: true, CAFile: "/nope"}.Client(); return err },
		"client bad CA":     func() error { _, err := Config{Enabled: true, CAFile: junk}.Client(); return err },
		"client cert without key": func() error {
			_, err := Config{Enabled: true, CertFile: certs.ClientCert}.Client()
			return err
		},
	}
	for name, f := range bad {
		if f() == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// handshake performs a TLS handshake between the given configs and returns
// the client-side error (nil on success).
func handshake(t *testing.T, server, client *tls.Config) error {
	t.Helper()
	lis, err := tls.Listen("tcp", "127.0.0.1:0", server)
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() {
		c, err := lis.Accept()
		if err != nil {
			return
		}
		c.(*tls.Conn).Handshake()
		io.Copy(io.Discard, c)
		c.Close()
	}()
	conn, err := tls.Dial("tcp", lis.Addr().String(), client)
	if err != nil {
		return err
	}
	defer conn.Close()
	// With TLS 1.3 a rejected client cert surfaces on first read.
	conn.Write([]byte("x"))
	one := make([]byte, 1)
	conn.SetReadDeadline(deadline())
	if _, err := conn.Read(one); err != nil && !isTimeout(err) {
		return err
	}
	return nil
}

func TestServerAuthAndMTLS(t *testing.T) {
	certs := testutil.NewCerts(t)

	plainSrv, err := Config{Enabled: true, CertFile: certs.ServerCert, KeyFile: certs.ServerKey}.Server()
	if err != nil {
		t.Fatal(err)
	}
	if plainSrv.MinVersion != tls.VersionTLS12 || plainSrv.ClientAuth != tls.NoClientCert {
		t.Errorf("unexpected server defaults: %+v", plainSrv)
	}
	trusting, _ := Config{Enabled: true, CAFile: certs.CA}.Client()
	if err := handshake(t, plainSrv, trusting); err != nil {
		t.Errorf("trusted server rejected: %v", err)
	}
	untrusting, _ := Config{Enabled: true, CAFile: certs.OtherCA}.Client()
	if err := handshake(t, plainSrv, untrusting); err == nil {
		t.Error("server signed by unknown CA was accepted")
	}
	skip, _ := Config{Enabled: true, InsecureSkipVerify: true}.Client()
	if err := handshake(t, plainSrv, skip); err != nil {
		t.Errorf("insecure_skip_verify: %v", err)
	}
	wrongName, _ := Config{Enabled: true, CAFile: certs.CA, ServerName: "evil.example"}.Client()
	if err := handshake(t, plainSrv, wrongName); err == nil {
		t.Error("server_name mismatch was accepted")
	}

	mtlsSrv, err := Config{Enabled: true, CertFile: certs.ServerCert, KeyFile: certs.ServerKey, CAFile: certs.CA}.Server()
	if err != nil {
		t.Fatal(err)
	}
	if mtlsSrv.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatal("ca_file on a server must require client certs")
	}
	if err := handshake(t, mtlsSrv, trusting); err == nil {
		t.Error("mTLS server accepted a client without a certificate")
	}
	withCert, err := Config{Enabled: true, CAFile: certs.CA, CertFile: certs.ClientCert, KeyFile: certs.ClientKey}.Client()
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(t, mtlsSrv, withCert); err != nil {
		t.Errorf("mTLS with valid client cert failed: %v", err)
	}
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

func deadline() time.Time { return time.Now().Add(200 * time.Millisecond) }
