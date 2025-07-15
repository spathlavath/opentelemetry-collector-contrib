// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package newrelicmysqlreceiver implements a receiver that can fetch stats from a MySQL database
// with New Relic enhanced monitoring capabilities including query performance tracking,
// wait events monitoring, and advanced InnoDB metrics.
package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"
