package analytics

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Harisankar2000/linkpulse/internal/config"
)

// KafkaOpts returns franz-go options for a secured, hosted Kafka cluster
// (e.g. Aiven or Confluent): TLS, optional mutual-TLS client certificates, and
// SASL. The zero value means plaintext, as in local Docker Compose.
func KafkaOpts(a config.KafkaAuth) ([]kgo.Opt, error) {
	var opts []kgo.Opt

	if a.TLS || a.CACert != "" || a.ClientCert != "" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if a.CACert != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(pemValue(a.CACert))) {
				return nil, errors.New("kafka: KAFKA_CA_CERT is not a valid PEM certificate")
			}
			tc.RootCAs = pool
		}
		if a.ClientCert != "" || a.ClientKey != "" {
			cert, err := tls.X509KeyPair([]byte(pemValue(a.ClientCert)), []byte(pemValue(a.ClientKey)))
			if err != nil {
				return nil, fmt.Errorf("kafka: client certificate: %w", err)
			}
			tc.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}

	switch strings.ToUpper(a.SASLMechanism) {
	case "":
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: a.Username, Pass: a.Password}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: a.Username, Pass: a.Password}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: a.Username, Pass: a.Password}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", a.SASLMechanism)
	}
	return opts, nil
}

// pemValue accepts certificates pasted either with real newlines or with
// literal "\n" sequences, since hosting dashboards handle multi-line values
// differently.
func pemValue(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), `\n`, "\n")
}
