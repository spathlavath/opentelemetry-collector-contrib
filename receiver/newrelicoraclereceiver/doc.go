// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package newrelicoraclereceiver implements a receiver for Oracle Database metrics
// that matches the comprehensive metric collection provided by the New Relic Oracle integration.
//
// This receiver connects to Oracle databases and collects metrics including:
//   - SGA (System Global Area) metrics: buffer cache hit ratios, shared pool statistics
//   - Memory metrics: PGA usage, allocation, and freeable memory
//   - Disk I/O metrics: physical reads/writes, block operations, timing
//   - Query performance: transaction rates, parse statistics, execution metrics
//   - Database health: session counts, CPU usage, wait times, response times
//   - Tablespace metrics: usage percentages, space allocation, offline detection
//   - Network metrics: traffic volume, I/O requests and throughput
//   - Instance metrics: long-running queries, locked accounts, background processes
//
// The receiver is designed to provide the same level of monitoring capabilities
// as the New Relic Oracle integration but in OpenTelemetry format.
package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"
