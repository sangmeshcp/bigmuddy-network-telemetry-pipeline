//go:build !noiceberg

package main

// The Iceberg output brings in Apache Arrow and the AWS SDK (~44 MiB of
// binary). Build with -tags noiceberg for a slim binary without it.
import _ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/iceberg"
