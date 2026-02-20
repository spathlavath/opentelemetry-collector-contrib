// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package models // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"

type SystemMetric struct {
	InstanceID string
	MetricName string
	Value      float64
}
