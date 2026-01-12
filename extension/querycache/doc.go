// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package querycache provides an extension that caches query performance data
// to enable coordination between metrics and logs pipelines, eliminating duplicate database queries.
package querycache // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/querycache"
