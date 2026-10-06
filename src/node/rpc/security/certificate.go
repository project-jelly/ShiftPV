package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

const (
	CertificateAnnotation = "shiftpv.io/node-rpc-certificate"
	PortAnnotation        = "shiftpv.io/node-rpc-port"
	Audience              = "shiftpv-node"
)

// NewCertificate keeps the key in this process. The authenticated Pod API
// publishes the public certificate, which clients trust for that incarnation.
func NewCertificate(podUID string) (tls.Certificate, string, error) {
	if podUID == "" {
		return tls.Certificate{}, "", fmt.Errorf("missing Node Pod UID")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, "", err
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: podUID}, DNSNames: []string{podUID}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(10 * 365 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, "", err
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, string(encoded), nil
}

func ClientTLS(podUID, certificate string) (*tls.Config, error) {
	block, rest := pem.Decode([]byte(certificate))
	if podUID == "" || block == nil || len(rest) != 0 || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("invalid Node certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	if cert.Subject.CommonName != podUID || cert.VerifyHostname(podUID) != nil {
		return nil, fmt.Errorf("Node certificate identity changed")
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: podUID}, nil
}
