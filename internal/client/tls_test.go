package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue returns a PEM certificate and key signed by the CA.
func (ca testCA) issue(t *testing.T, usage x509.ExtKeyUsage, dnsNames ...string) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{usage}, DNSNames: dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func writeFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// startTLSServer accepts connections and completes handshakes; it requires a
// client certificate signed by clientCA when clientCA is set.
func startTLSServer(t *testing.T, ca testCA, clientCA *testCA) string {
	t.Helper()
	certPEM, keyPEM := ca.issue(t, x509.ExtKeyUsageServerAuth, "valkey.example")
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	if clientCA != nil {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = x509.NewCertPool()
		config.ClientCAs.AddCert(clientCA.cert)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = conn.(*tls.Conn).Handshake()
				conn.Close()
			}()
		}
	}()
	return listener.Addr().String()
}

func handshake(address string, config *tls.Config) error {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", address, config)
	if err != nil {
		return err
	}
	defer conn.Close()
	// TLS 1.3 reports a rejected client certificate on the first read.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		return err
	}
	return nil
}

func TestTlsConfigVerifiesServerWithCA(t *testing.T) {
	ca := newTestCA(t)
	address := startTLSServer(t, ca, nil)
	caFile := writeFile(t, "ca.crt", ca.pem)

	config, err := getTlsConfig(TlsConfig{CACertFilePath: caFile, ServerName: "valkey.example"}, address)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(address, config); err != nil {
		t.Fatalf("a server certificate for server_name must be accepted: %v", err)
	}

	config, err = getTlsConfig(TlsConfig{CACertFilePath: caFile}, address)
	if err != nil {
		t.Fatal(err)
	}
	if config.ServerName != "127.0.0.1" {
		t.Fatalf("server name defaults to the address host, got %q", config.ServerName)
	}
	if err := handshake(address, config); err == nil {
		t.Fatal("a certificate that does not name the address host must be rejected")
	}

	otherCA := writeFile(t, "other.crt", newTestCA(t).pem)
	config, err = getTlsConfig(TlsConfig{CACertFilePath: otherCA, ServerName: "valkey.example"}, address)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(address, config); err == nil {
		t.Fatal("a certificate from another CA must be rejected")
	}
}

func TestTlsConfigWithoutCADoesNotVerify(t *testing.T) {
	ca := newTestCA(t)
	address := startTLSServer(t, ca, nil)
	config, err := getTlsConfig(TlsConfig{}, address)
	if err != nil {
		t.Fatal(err)
	}
	if !config.InsecureSkipVerify {
		t.Fatal("without ca_cert the server is not verified")
	}
	if err := handshake(address, config); err != nil {
		t.Fatal(err)
	}
}

func TestTlsConfigClientCertificate(t *testing.T) {
	ca, clientCA := newTestCA(t), newTestCA(t)
	address := startTLSServer(t, ca, &clientCA)
	certPEM, keyPEM := clientCA.issue(t, x509.ExtKeyUsageClientAuth)
	certFile, keyFile := writeFile(t, "tls.crt", certPEM), writeFile(t, "tls.key", keyPEM)

	// A client certificate no longer requires ca_cert, and vice versa.
	config, err := getTlsConfig(TlsConfig{CertFilePath: certFile, KeyFilePath: keyFile}, address)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(address, config); err != nil {
		t.Fatalf("the client certificate must be presented: %v", err)
	}
	config, err = getTlsConfig(TlsConfig{}, address)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(address, config); err == nil {
		t.Fatal("the server requires a client certificate")
	}

	if _, err := getTlsConfig(TlsConfig{CertFilePath: certFile}, address); err == nil {
		t.Fatal("cert without key must be rejected")
	}
	if _, err := getTlsConfig(TlsConfig{CACertFilePath: writeFile(t, "empty.crt", []byte("not a PEM"))}, address); err == nil {
		t.Fatal("a ca_cert without certificates must be rejected")
	}
}
