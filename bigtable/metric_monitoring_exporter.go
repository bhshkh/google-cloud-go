/*
Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bigtable

import (
	"context"
	"fmt"
	"log"

	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/sdk/metric"
	otelmetricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	meterName            = "bigtable.googleapis.com/internal/client/"
	bigtableResourceType = "bigtable_client_raw"

	// The number of timeserieses to send to GCM in a single request. This
	// is a hard limit in the GCM API, so we never want to exceed 200.
	sendBatchSize = 200
)

var (
	monitoredResLabelsSet = map[string]bool{
		monitoredResLabelKeyProject:  true,
		monitoredResLabelKeyInstance: true,
		monitoredResLabelKeyCluster:  true,
		monitoredResLabelKeyTable:    true,
		monitoredResLabelKeyZone:     true,
	}

	errShutdown = fmt.Errorf("exporter is shutdown")
)

type errUnexpectedAggregationKind struct {
	kind string
}

func (e errUnexpectedAggregationKind) Error() string {
	return fmt.Sprintf("the metric kind is unexpected: %v", e.kind)
}

type bigtableMetricsHandler struct {
	exporter otelmetric.Exporter
}

func newBigtableMetricsHandler(ctx context.Context, config *metricsConfigInternal) (*bigtableMetricsHandler, error) {
	osExporter, err := mexporter.New(
		mexporter.WithProjectID(config.project),
		mexporter.WithCreateServiceTimeSeries(),
		mexporter.WithMetricDescriptorTypeFormatter(func(desc otelmetricdata.Metrics) string {
			return fmt.Sprintf("%s%s", meterName, desc.Name)
		}),
		mexporter.WithDisableCreateMetricDescriptors(),
		mexporter.WithMonitoredResourceDescription(mexporter.MonitoredResourceDescription{
			Type: "bigtable_client_raw",
		}),
		mexporter.WithMonitoredResourceAttributes(attribute.NewAllowKeysFilter("project_id", "instance", "table", "cluster", "zone")),
	)
	if err != nil {
		log.Fatalf("Failed to create exporter: %v", err)
	}

	return &bigtableMetricsHandler{
		exporter: osExporter,
	}, nil
}
