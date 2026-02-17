// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package newrelicmysqlreceiver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

func init() {
	if os.Getenv("DOCKER_HOST") == "" {
		if _, err := os.Stat(os.ExpandEnv("$HOME/.rd/docker.sock")); err == nil {
			os.Setenv("DOCKER_HOST", "unix://"+os.ExpandEnv("$HOME/.rd/docker.sock"))
		}
	}
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

func TestIntegrationGaleraCluster(t *testing.T) {
	testCases := []struct {
		desc                    string
		configFilename          string
		expectedResultsFilename string
		containerRequest        testcontainers.ContainerRequest
	}{
		{
			desc:                    "Galera cluster metrics collection",
			configFilename:          "galera_config.yaml",
			expectedResultsFilename: "galera_expected_metrics.yaml",
			containerRequest:        galeraContainerRequest,
		},
	}

	factories, err := otelcoltest.NopFactories()
	require.NoError(t, err)

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			// Start the container
			container := getContainer(t, testCase.containerRequest)
			defer func() {
				require.NoError(t, container.Terminate(context.Background()))
			}()

			// Get the container's host and port
			host, err := container.Host(context.Background())
			require.NoError(t, err)
			port, err := container.MappedPort(context.Background(), "3306")
			require.NoError(t, err)

			// Wait for Galera to be fully ready
			time.Sleep(15 * time.Second)

			// Create factory and load config
			factory := NewFactory()
			factories.Receivers[metadata.Type] = factory
			configFile := filepath.Join("testdata", "integration", testCase.configFilename)
			cfg, err := otelcoltest.LoadConfigAndValidate(configFile, factories)
			require.NoError(t, err)

			mysqlConfig := cfg.Receivers[component.NewID(metadata.Type)].(*Config)
			// Update config with dynamic container endpoint
			mysqlConfig.Endpoint = host + ":" + port.Port()

			// Create receiver and consumer
			consumer := new(consumertest.MetricsSink)
			settings := receivertest.NewNopSettings(metadata.Type)
			rcvr, err := factory.CreateMetrics(context.Background(), settings, mysqlConfig, consumer)
			require.NoError(t, err, "failed creating metrics receiver")

			// Start receiver
			require.NoError(t, rcvr.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				require.NoError(t, rcvr.Shutdown(context.Background()))
			}()

			// Wait for metrics to be collected
			require.Eventuallyf(t, func() bool {
				return len(consumer.AllMetrics()) > 0
			}, 2*time.Minute, 1*time.Second, "failed to receive more than 0 metrics")

			// Validate collected metrics
			actualMetrics := consumer.AllMetrics()[0]
			expectedFile := filepath.Join("testdata", "integration", testCase.expectedResultsFilename)
			expectedMetrics, err := golden.ReadMetrics(expectedFile)
			require.NoError(t, err)

			err = pmetrictest.CompareMetrics(expectedMetrics, actualMetrics,
				pmetrictest.IgnoreMetricsOrder(),
				pmetrictest.IgnoreTimestamp(),
				pmetrictest.IgnoreStartTimestamp(),
				pmetrictest.IgnoreMetricValues(),
				pmetrictest.IgnoreMetricDataPointsOrder(),
				pmetrictest.IgnoreSubsequentDataPoints(),
			)
			require.NoError(t, err)
		})
	}
}

func TestIntegrationMySQL(t *testing.T) {
	testCases := []struct {
		desc                    string
		configFilename          string
		expectedResultsFilename string
		containerRequest        testcontainers.ContainerRequest
	}{
		{
			desc:                    "MySQL standard metrics collection",
			configFilename:          "mysql_config.yaml",
			expectedResultsFilename: "mysql_expected_metrics.yaml",
			containerRequest:        mysqlContainerRequest,
		},
	}

	factories, err := otelcoltest.NopFactories()
	require.NoError(t, err)

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			// Start the container
			container := getContainer(t, testCase.containerRequest)
			defer func() {
				require.NoError(t, container.Terminate(context.Background()))
			}()

			// Get the container's host and port
			host, err := container.Host(context.Background())
			require.NoError(t, err)
			port, err := container.MappedPort(context.Background(), "3306")
			require.NoError(t, err)

			// Wait for MySQL to be fully ready
			time.Sleep(10 * time.Second)

			// Create factory and load config
			factory := NewFactory()
			factories.Receivers[metadata.Type] = factory
			configFile := filepath.Join("testdata", "integration", testCase.configFilename)
			cfg, err := otelcoltest.LoadConfigAndValidate(configFile, factories)
			require.NoError(t, err)

			mysqlConfig := cfg.Receivers[component.NewID(metadata.Type)].(*Config)
			// Update config with dynamic container endpoint
			mysqlConfig.Endpoint = host + ":" + port.Port()

			// Create receiver and consumer
			consumer := new(consumertest.MetricsSink)
			settings := receivertest.NewNopSettings(metadata.Type)
			rcvr, err := factory.CreateMetrics(context.Background(), settings, mysqlConfig, consumer)
			require.NoError(t, err, "failed creating metrics receiver")

			// Start receiver
			require.NoError(t, rcvr.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				require.NoError(t, rcvr.Shutdown(context.Background()))
			}()

			// Wait for metrics to be collected
			require.Eventuallyf(t, func() bool {
				return len(consumer.AllMetrics()) > 0
			}, 2*time.Minute, 1*time.Second, "failed to receive more than 0 metrics")

			// Validate collected metrics
			actualMetrics := consumer.AllMetrics()[0]
			expectedFile := filepath.Join("testdata", "integration", testCase.expectedResultsFilename)
			expectedMetrics, err := golden.ReadMetrics(expectedFile)
			require.NoError(t, err)

			err = pmetrictest.CompareMetrics(expectedMetrics, actualMetrics,
				pmetrictest.IgnoreMetricsOrder(),
				pmetrictest.IgnoreTimestamp(),
				pmetrictest.IgnoreStartTimestamp(),
				pmetrictest.IgnoreMetricValues(),
				pmetrictest.IgnoreMetricDataPointsOrder(),
				pmetrictest.IgnoreSubsequentDataPoints(),
			)
			require.NoError(t, err)
		})
	}
}

var galeraContainerRequest = testcontainers.ContainerRequest{
	Image:        "mariadb:11.3",
	ExposedPorts: []string{"3306/tcp"},
	Env: map[string]string{
		"MYSQL_ROOT_PASSWORD": "galerapass",
		"MYSQL_DATABASE":      "employees",
	},
	Cmd: []string{
		"--wsrep-new-cluster",
		"--wsrep-on=ON",
		"--wsrep-provider=/usr/lib/galera/libgalera_smm.so",
		"--wsrep-cluster-name=test-cluster",
		"--wsrep-cluster-address=gcomm://",
		"--wsrep-node-name=test-node",
		"--binlog-format=ROW",
		"--default-storage-engine=InnoDB",
		"--innodb-autoinc-lock-mode=2",
	},
	WaitingFor: wait.ForLog("ready for connections").
		WithStartupTimeout(2 * time.Minute),
}

var mysqlContainerRequest = testcontainers.ContainerRequest{
	Image:        "mysql:8.4",
	ExposedPorts: []string{"3306/tcp"},
	Env: map[string]string{
		"MYSQL_ROOT_PASSWORD": "rootpassword",
		"MYSQL_DATABASE":      "employees",
	},
	WaitingFor: wait.ForLog("ready for connections").
		WithOccurrence(2).
		WithStartupTimeout(2 * time.Minute),
}

func getContainer(t *testing.T, req testcontainers.ContainerRequest) testcontainers.Container {
	require.NoError(t, req.Validate())

	ctx := context.Background()

	container, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
	require.NoError(t, err)
	return container
}
