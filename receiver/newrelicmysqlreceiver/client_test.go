// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configtls"
)

func TestNewMySQLClient(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid config with insecure TLS",
			cfg: &Config{
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				AddrConfig: confignet.AddrConfig{
					Endpoint:  "localhost:3306",
					Transport: confignet.TransportTypeTCP,
				},
				TLS: configtls.ClientConfig{
					Insecure: true,
				},
				AllowNativePasswords: true,
			},
			wantErr: false,
		},
		{
			name: "valid config with secure TLS disabled",
			cfg: &Config{
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				AddrConfig: confignet.AddrConfig{
					Endpoint:  "localhost:3306",
					Transport: confignet.TransportTypeTCP,
				},
				TLS: configtls.ClientConfig{
					Insecure: true,
				},
				AllowNativePasswords: false,
			},
			wantErr: false,
		},
		{
			name: "valid config with unix socket",
			cfg: &Config{
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				AddrConfig: confignet.AddrConfig{
					Endpoint:  "/var/run/mysqld/mysqld.sock",
					Transport: "unix",
				},
				TLS: configtls.ClientConfig{
					Insecure: true,
				},
				AllowNativePasswords: true,
			},
			wantErr: false,
		},
		{
			name: "empty password is valid",
			cfg: &Config{
				Username: "testuser",
				Password: "",
				Database: "testdb",
				AddrConfig: confignet.AddrConfig{
					Endpoint:  "localhost:3306",
					Transport: confignet.TransportTypeTCP,
				},
				TLS: configtls.ClientConfig{
					Insecure: true,
				},
				AllowNativePasswords: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewMySQLClient(tt.cfg)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}

func TestMySQLClient_Close_NilDB(t *testing.T) {
	// Create a client directly without connecting
	client := &mySQLClient{
		connStr: "test:test@tcp(localhost:3306)/testdb",
		db:      nil,
	}

	err := client.Close()
	assert.NoError(t, err, "Close should not error when db is nil")
}

func TestConfig_Unmarshal_Nil(t *testing.T) {
	cfg := &Config{}
	err := cfg.Unmarshal(nil)
	assert.NoError(t, err)
}
