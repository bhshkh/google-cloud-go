package bigtable

import (
	"fmt"

	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const meterName = "bigtable.googleapis.com/internal/client/"

type monitoringExporter struct {
	exporter sdkmetric.Exporter
}

func newMonitoringMetricExporter(config *openTelemetryConfig) (*monitoringExporter, error) {
	exporter, err := mexporter.New(
		mexporter.WithProjectID(config.project),
		mexporter.WithMetricDescriptorTypeFormatter(func(desc metricdata.Metrics) string {
			return fmt.Sprintf("%v%s", meterName, desc.Name)
		}))
	if err != nil {
		return nil, err
	}
	return &monitoringExporter{
		exporter: exporter,
	}, nil
}
