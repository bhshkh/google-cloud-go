package bigtable

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigtable/internal"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

const (
	metricsPrefix       = "bigtable/"
	locationMetadataKey = "x-goog-ext-425905942-bin"

	// Metric attribute keys for labels
	attributeKeyProject            = attribute.Key("project_id")
	attributeKeyInstance           = attribute.Key("instance")
	attributeKeyTable              = attribute.Key("table")
	attributeKeyCluster            = attribute.Key("cluster")
	attributeKeyAppProfile         = attribute.Key("app_profile")
	attributeKeyZone               = attribute.Key("zone")
	attributeKeyMethod             = attribute.Key("method")
	attributeKeyOperationStatus    = attribute.Key("status")
	attributeKeyStreamingOperation = attribute.Key("streaming")
	attributeKeyClientName         = attribute.Key("client_name")
	attributeKeyClientUID          = attribute.Key("client_uid")

	// Metric names
	metricNameOperationLatencies = "operation_latencies"
	metricNameAttemptLatencies   = "attempt_latencies"
	metricNameCounter            = "test_counter"

	bigtableResourceType = "bigtable_client_raw"
)

var (
	attributeValueClientName = fmt.Sprintf("cloud.google.com/go/bigtable v%v", internal.Version)

	// openTelemetryMetricsEnabled is used to track if OpenTelemetry Metrics need to be recorded
	openTelemetryMetricsEnabled = false

	// mutex to avoid data race in reading/writing the above flag
	otMu = sync.RWMutex{}
)

type instrumentAttributes struct {
	tableName       string
	appProfileId    string
	methodName      string
	isStreaming     bool
	status          *string
	grpcCallOptions []grpc.CallOption
}

func newInstrumentAttributes(methodName, tableName, appProfile string, isStreaming bool) instrumentAttributes {
	headerMD := metadata.New(nil)
	trailerMD := metadata.New(nil)
	status := ""
	grpcCallOptions := []grpc.CallOption{grpc.Header(&headerMD), grpc.Trailer(&trailerMD)}

	return instrumentAttributes{
		tableName:       tableName,
		appProfileId:    appProfile,
		methodName:      methodName,
		isStreaming:     isStreaming,
		status:          &status,
		grpcCallOptions: grpcCallOptions,
	}
}

// IsOpenTelemetryMetricsEnabled tells whether OpenTelemtery metrics is enabled or not.
func IsOpenTelemetryMetricsEnabled() bool {
	otMu.RLock()
	defer otMu.RUnlock()
	return openTelemetryMetricsEnabled
}

// EnableBuiltinMetrics enables OpenTelemetery metrics
func EnableBuiltinMetrics() {
	setOpenTelemetryMetricsFlag(true)
}

func setOpenTelemetryMetricsFlag(enable bool) {
	otMu.Lock()
	openTelemetryMetricsEnabled = enable
	otMu.Unlock()
}

func generateClientUID() string {
	hostname := "localhost"
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println(err)
	}
	return "go-" + uuid.NewString() + "@" + hostname

}

func createOpenTelemetryConfig(ctx context.Context, mp metric.MeterProvider, project, instance string) (*openTelemetryConfig, error) {
	config := &openTelemetryConfig{
		commonAttributes: []attribute.KeyValue{},
	}

	if !IsOpenTelemetryMetricsEnabled() {
		return config, nil
	}
	// Construct attributes for Metrics
	attributeMap := []attribute.KeyValue{
		attributeKeyProject.String(project),
		attributeKeyInstance.String(instance),
		attributeKeyClientUID.String(generateClientUID()),
		attributeKeyClientName.String(attributeValueClientName),
	}
	config.commonAttributes = append(config.commonAttributes, attributeMap...)
	config.project = project
	err := setOpenTelemetryMetricProvider(ctx, config, mp)
	if err != nil {
		return config, err
	}
	return config, nil
}

func setOpenTelemetryMetricProvider(ctx context.Context, config *openTelemetryConfig, mp metric.MeterProvider) error {
	if mp == nil {
		// Fallback to default meter provider if none provided
		// defaultExporter, err := newMonitoringMetricExporter(config)
		// if err != nil {
		// 	return err
		// }

		defaultExporter, err := stdoutmetric.New()
		if err != nil {
			return err
		}

		res, err := resource.New(
			ctx,
			// Use the GCP resource detector to detect information about the GCP platform
			resource.WithDetectors(gcp.NewDetector()),
			// Keep the default detectors
			resource.WithTelemetrySDK(),
			// Add attributes from environment variables
			resource.WithFromEnv(),
			// Add your own custom attributes to identify your application
			resource.WithAttributes(
				semconv.ServiceNameKey.String("test-servicename"),
			),
		)
		if err != nil {
			return err
		}
		mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(defaultExporter,
				sdkmetric.WithInterval(3*time.Second))), // TODO: Remove this, use default 60s
			sdkmetric.WithResource(res),
		)
	}
	config.meterProvider = mp
	return initializeMetricInstruments(config)
}

func initializeMetricInstruments(config *openTelemetryConfig) error {
	if !IsOpenTelemetryMetricsEnabled() {
		return nil
	}
	meter := config.meterProvider.Meter(meterName)

	// Initialize operation latencies
	operationLatencies, err := meter.Int64Histogram(
		metricNameOperationLatencies,
		metric.WithDescription("Total time until final operation success or failure, including retries and backoff."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}
	config.metricOperationLatencies = operationLatencies

	// Initialize attempt latencies
	attemptLatencies, err := meter.Int64Histogram(
		metricNameAttemptLatencies,
		metric.WithDescription("Client observed latency per RPC attempt."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}
	config.metricAttemptLatencies = attemptLatencies

	return nil
}

func (otConfig *openTelemetryConfig) recordOperationLatency(ctx context.Context, attr *instrumentAttributes) func() error {
	startTime := time.Now()
	return func() error {
		elapsedTime := time.Since(startTime).Milliseconds()
		if !IsOpenTelemetryMetricsEnabled() || otConfig == nil || otConfig.metricOperationLatencies == nil {
			return nil
		}

		attrMap, err := createAttributeMap(attr)
		otConfig.metricOperationLatencies.Record(ctx, elapsedTime, metric.WithAttributes(append(otConfig.commonAttributes, attrMap...)...))
		return err
	}
}

func createAttributeMap(attr *instrumentAttributes) ([]attribute.KeyValue, error) {
	clusterId, zoneId, err := obtainLocationFromMetadata(attr.grpcCallOptions)
	if err != nil {
		return nil, err
	}
	attributeMap := []attribute.KeyValue{
		attributeKeyMethod.String(attr.methodName),
		attributeKeyTable.String(attr.tableName),
		attributeKeyAppProfile.String(attr.appProfileId),
		attributeKeyStreamingOperation.Bool(attr.isStreaming),
		attributeKeyOperationStatus.String(*attr.status),
		attributeKeyCluster.String(clusterId),
		attributeKeyCluster.String(zoneId),
	}
	return attributeMap, err
}

// Obtain cluster and zone from metadata
func obtainLocationFromMetadata(grpcCallOptions []grpc.CallOption) (string, string, error) {
	// Check whether location metadata available in response header metadata
	headerOption := grpcCallOptions[0].(grpc.HeaderCallOption)
	headerMD := headerOption.HeaderAddr
	locationMetadata := headerMD.Get(locationMetadataKey)
	if locationMetadata == nil {
		// Check whether location metadata available in response trailer metadata
		// if none found in response header metadata
		trailerOption := grpcCallOptions[1].(grpc.TrailerCallOption)
		trailerMD := trailerOption.TrailerAddr
		locationMetadata = trailerMD.Get(locationMetadataKey)
	}

	if len(locationMetadata) < 1 {
		return "", "", fmt.Errorf("Failed to get location metadata")
	}

	// Unmarshal binary location metadata
	responseParams := &btpb.ResponseParams{}
	err := proto.Unmarshal([]byte(locationMetadata[0]), responseParams)
	if err != nil {
		return "", "", err
	}

	return responseParams.GetClusterId(), responseParams.GetZoneId(), nil
}
