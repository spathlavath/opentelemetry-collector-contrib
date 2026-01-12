#!/usr/bin/env bash

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

set -e

# This script adds the querycache extension factory to the generated components.go
# The extension is embedded in the newrelicsqlserverreceiver package, so it's not
# automatically picked up by the OpenTelemetry Collector Builder.

COMPONENTS_FILE="cmd/otelcontribcol/components.go"

echo "Adding query cache extension to $COMPONENTS_FILE"

# Check if the extension is already added (to make the script idempotent)
if grep -q "newrelicsqlserverreceiver.NewExtensionFactory()" "$COMPONENTS_FILE"; then
    echo "Query cache extension already present in $COMPONENTS_FILE"
    exit 0
fi

# Use sed to add the extension factory to the Extensions map
# We need to add it after healthcheckextension.NewFactory()
# macOS and Linux sed have different syntax, so we use perl for portability
perl -i -pe 's/(healthcheckextension\.NewFactory\(\),)/$1\n\t\tnewrelicsqlserverreceiver.NewExtensionFactory(), \/\/ Query cache extension/' "$COMPONENTS_FILE"

# Also add the extension to the ExtensionModules map
perl -i -pe 's/(factories\.ExtensionModules\[healthcheckextension\.NewFactory\(\)\.Type\(\)\] = "[^"]*")/$1\n\tfactories.ExtensionModules[newrelicsqlserverreceiver.NewExtensionFactory().Type()] = "github.com\/open-telemetry\/opentelemetry-collector-contrib\/receiver\/newrelicsqlserverreceiver v0.141.0"/' "$COMPONENTS_FILE"

echo "Successfully added query cache extension to $COMPONENTS_FILE"
