package analytics

import (
	"testing"

	"github.com/Harisankar2000/linkpulse/internal/config"
)

func TestKafkaOpts(t *testing.T) {
	tests := []struct {
		name    string
		auth    config.KafkaAuth
		wantN   int
		wantErr bool
	}{
		{"plaintext", config.KafkaAuth{}, 0, false},
		{"tls only", config.KafkaAuth{TLS: true}, 1, false},
		{"tls + scram", config.KafkaAuth{TLS: true, SASLMechanism: "scram-sha-256", Username: "u", Password: "p"}, 2, false},
		{"plain sasl", config.KafkaAuth{SASLMechanism: "PLAIN", Username: "u", Password: "p"}, 1, false},
		{"bad mechanism", config.KafkaAuth{SASLMechanism: "GSSAPI"}, 0, true},
		{"bad ca", config.KafkaAuth{CACert: "not a cert"}, 0, true},
		{"bad client cert", config.KafkaAuth{ClientCert: "x", ClientKey: "y"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := KafkaOpts(tt.auth)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(opts) != tt.wantN {
				t.Fatalf("got %d options, want %d", len(opts), tt.wantN)
			}
		})
	}
}

func TestPemValueUnescapesNewlines(t *testing.T) {
	if got := pemValue(`  -----BEGIN X-----\nabc\n-----END X-----  `); got != "-----BEGIN X-----\nabc\n-----END X-----" {
		t.Fatalf("got %q", got)
	}
}
