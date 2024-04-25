package bigtable

import (
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"cloud.google.com/go/bigtable/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
)

const (
	OtInstrumentationScope = "cloud.google.com/go"
	metricsPrefix          = "bigtable/"
)

var (
	attributeKeyProject  = attribute.Key("project_id")
	attributeKeyInstance = attribute.Key("instance_id")
	attributeKeyMethod   = attribute.Key("grpc_client_method")
)

func createOpenTelemetryConfig(project, instance string) (*openTelemetryConfig, error) {
	config := &openTelemetryConfig{
		attributeMap: []attribute.KeyValue{
			// Construct attributes for Metrics
			attributeKeyProject.String(project),
			attributeKeyInstance.String(instance),
		},
	}

	metricExporter, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	config.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			// Default is 1m. Set to 3s for demonstrative purposes.
			sdkmetric.WithInterval(3*time.Second))),
	)
	err = initializeMetricInstruments(config)
	return config, err
}

func initializeMetricInstruments(config *openTelemetryConfig) error {

	meter := config.meterProvider.Meter(OtInstrumentationScope, metric.WithInstrumentationVersion(internal.Version))

	readRowsCount, err := meter.Int64Counter(
		"api.counter",
		metric.WithDescription("readRowsCount"),
		metric.WithUnit("{call}"),
	)
	if err != nil {
		return err
	}
	config.readRowsCount = readRowsCount
	return nil
}
