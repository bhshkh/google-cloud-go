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
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable/internal"
	"github.com/google/uuid"
	gax "github.com/googleapis/gax-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	// instrumentationScope is the instrumentation name that will be associated with the emitted telemetry.
	instrumentationScope = "cloud.google.com/go"

	// duration between two metric exports
	// samplePeriod = 5 * time.Minute
	samplePeriod = 15 * time.Second

	metricsPrefix       = "bigtable/"
	locationMetadataKey = "x-goog-ext-425905942-bin"

	// Monitored resource labels
	monitoredResLabelKeyProject  = "project_id"
	monitoredResLabelKeyInstance = "instance"
	monitoredResLabelKeyTable    = "table"
	monitoredResLabelKeyCluster  = "cluster"
	monitoredResLabelKeyZone     = "zone"

	// Metric labels
	metricLabelKeyAppProfile         = "app_profile"
	metricLabelKeyMethod             = "method"
	metricLabelKeyOperationStatus    = "status"
	metricLabelKeyStreamingOperation = "streaming"
	metricLabelKeyClientName         = "client_name"
	metricLabelKeyClientUID          = "client_uid"

	// Metric names
	metricNameOperationLatencies = "operation_latencies"
	metricNameAttemptLatencies   = "attempt_latencies"
	metricNameRetryCount         = "retry_count"
	metricNameServerLatencies    = "server_latencies"
)

var (
	clientName = fmt.Sprintf("cloud.google.com/go/bigtable v%v", internal.Version)

	metricAttrsCommon = map[string]bool{
		monitoredResLabelKeyProject:  true,
		monitoredResLabelKeyInstance: true,
		monitoredResLabelKeyTable:    true,
		monitoredResLabelKeyCluster:  true,
		monitoredResLabelKeyZone:     true,
		metricLabelKeyAppProfile:     true,
		metricLabelKeyMethod:         true,
		metricLabelKeyClientName:     true,
		metricLabelKeyClientUID:      true,
	}

	// All the built-in metrics have same attributes except 'status' and 'streaming'
	// These attributes need to be added to only few of the metrics
	builtinMetrics = map[string]metricInfo{
		metricNameOperationLatencies: {
			desc:       "Total time until final operation success or failure, including retries and backoff.",
			metricType: metricdata.Histogram[float64]{},
			unit:       "ns",
			additionalAttributes: []string{
				metricLabelKeyOperationStatus,
				metricLabelKeyStreamingOperation,
			},
		},
		metricNameAttemptLatencies: {
			desc:       "Client observed latency per RPC attempt.",
			metricType: metricdata.Histogram[float64]{},
			unit:       "ns",
			additionalAttributes: []string{
				metricLabelKeyOperationStatus,
				metricLabelKeyStreamingOperation,
			},
		},
		metricNameServerLatencies: {
			desc:       "The latency measured from the moment that the RPC entered the Google data center until the RPC was completed.",
			metricType: metricdata.Histogram[float64]{},
			unit:       "ns",
			additionalAttributes: []string{
				metricLabelKeyOperationStatus,
				metricLabelKeyStreamingOperation,
			},
		},
		metricNameRetryCount: {
			desc:       "The number of additional RPCs sent after the initial attempt.",
			metricType: metricdata.Sum[int64]{},
			additionalAttributes: []string{
				metricLabelKeyOperationStatus,
			},
		},
	}
)

type metricInfo struct {
	desc                 string
	metricType           metricdata.Aggregation
	unit                 string
	additionalAttributes []string
}

type builtInMetricInstruments struct {
	distributions map[string]metric.Float64Histogram // Key is metric name e.g. operation_latencies
	counters      map[string]metric.Int64Counter     // Key is metric name e.g. retry_count
}

// mergeMaps is a utility function to merge 2 maps into a new one
func mergeMaps(m1 map[string]bool, m2 map[string]bool) map[string]bool {
	m3 := make(map[string]bool)
	for k, v := range m1 {
		m3[k] = v
	}
	for k, v := range m2 {
		m3[k] = v
	}
	return m3
}

// Generates unique client ID in the format go-<random UUID>@<>hostname
func generateClientUID() string {
	hostname := "localhost"
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println(err)
	}
	return "go-" + uuid.NewString() + "@" + hostname

}

type metricsConfigInternal struct {
	project string

	// Flag to record and export built-in metrics
	builtInEnabled bool

	// attributes that are specific to a client instance and
	// do not change across different function calls on client
	clientAttributes []attribute.KeyValue

	// Contains one entry per meter provider
	instruments []builtInMetricInstruments
}

func newMetricsConfigInternal(ctx context.Context, project, instance string, userProvidedConfig *metricsConfig) (*metricsConfigInternal, error) {
	internalMetricsConfig := &metricsConfigInternal{
		project: project,
		clientAttributes: []attribute.KeyValue{
			attribute.String(monitoredResLabelKeyProject, project),
			attribute.String(monitoredResLabelKeyInstance, instance),
			attribute.String(metricLabelKeyClientUID, generateClientUID()),
			attribute.String(metricLabelKeyClientName, clientName),
		},
		instruments: []builtInMetricInstruments{},
	}

	// Create metrics handler
	metricsHandler, err := newBigtableMetricsHandler(ctx, internalMetricsConfig)
	if err != nil {
		return internalMetricsConfig, err
	}

	// Create default meter provider
	defaultMp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				metricsHandler.exporter,
				sdkmetric.WithInterval(samplePeriod),
			),
		),
	)

	userProvidedMeterProviders := []*sdkmetric.MeterProvider{}
	if userProvidedConfig != nil {
		internalMetricsConfig.builtInEnabled = userProvidedConfig.builtInEnabled
		userProvidedMeterProviders = userProvidedConfig.meterProviders
	} else {
		internalMetricsConfig.builtInEnabled = true
	}

	// Create instruments on all meter providers
	allMeterProviders := append(userProvidedMeterProviders, defaultMp)
	for _, mp := range allMeterProviders {
		builtInMetricInstruments := builtInMetricInstruments{
			distributions: make(map[string]metric.Float64Histogram),
			counters:      make(map[string]metric.Int64Counter),
		}

		// Create meter
		meter := mp.Meter(instrumentationScope, metric.WithInstrumentationVersion(internal.Version))

		// Create instruments
		for metricName, metricDetails := range builtinMetrics {
			if _, ok := metricDetails.metricType.(metricdata.Histogram[float64]); ok {
				builtInMetricInstruments.distributions[metricName], err = meter.Float64Histogram(
					metricName,
					metric.WithDescription(metricDetails.desc),
					metric.WithUnit(metricDetails.unit),
				)
				if err != nil {
					return internalMetricsConfig, err
				}
			} else if _, ok := metricDetails.metricType.(metricdata.Sum[int64]); ok {
				builtInMetricInstruments.counters[metricName], err = meter.Int64Counter(
					metricName,
					metric.WithDescription(metricDetails.desc),
					metric.WithUnit(metricDetails.unit),
				)
				if err != nil {
					return internalMetricsConfig, err
				}
			}
			internalMetricsConfig.instruments = append(internalMetricsConfig.instruments, builtInMetricInstruments)
		}
	}
	return internalMetricsConfig, nil
}

// builtinMetricsTracer is created one per function call
// It is used to store metric attribute values and other data required to obtain them
type builtinMetricsTracer struct {
	tableName    string
	appProfileID string
	method       string
	isStreaming  bool

	// gRPC status code
	status string

	// Contains the header and trailer response metadata which is used to extract cluster and zone
	headerTrailerMD []grpc.CallOption

	attemptCount int64
}

func newBuiltinMetricsTracer(method, tableName, appProfile string, isStreaming bool) builtinMetricsTracer {
	headerMD := metadata.New(nil)
	trailerMD := metadata.New(nil)
	headerTrailerMD := []grpc.CallOption{grpc.Header(&headerMD), grpc.Trailer(&trailerMD)}

	return builtinMetricsTracer{
		tableName:       tableName,
		appProfileID:    appProfile,
		method:          method,
		isStreaming:     isStreaming,
		status:          "",
		headerTrailerMD: headerTrailerMD,
		attemptCount:    0,
	}
}

func (mt *builtinMetricsTracer) recordAndConvertErr(err error) error {
	statusCode, statusErr := convertToGrpcStatusErr(err)
	mt.status = statusCode.String()
	return statusErr
}

var noOpRecordFn = func() error {
	return nil
}

func (config *metricsConfigInternal) attemptRecorder(ctx context.Context, mt *builtinMetricsTracer,
	f func(ctx context.Context, _ gax.CallSettings) error, opts ...gax.CallOption) error {

	callWrapper := func(ctx context.Context, callSettings gax.CallSettings) error {
		// Increment number of attempts
		mt.attemptCount++

		attemptRecorder := config.recordAttemptCompletion(ctx, mt)
		defer attemptRecorder()

		err := f(ctx, callSettings)

		// Record attempt status
		statusCode, _ := convertToGrpcStatusErr(err)
		mt.status = statusCode.String()
		return err
	}
	return gax.Invoke(ctx, callWrapper, opts...)
}

// recordAttemptCompletion returns a function that should be executed to record attempt specific metrics
func (config *metricsConfigInternal) recordAttemptCompletion(ctx context.Context, mt *builtinMetricsTracer) func() error {
	if !config.builtInEnabled {
		return noOpRecordFn
	}
	startTime := time.Now()

	return func() error {
		// Calculate elapsed time
		elapsedTime := time.Since(startTime).Nanoseconds()

		// Attributes for attempt_latencies
		attemptLatCurrCallAttrs, err := newOtelMetricAttrs(mt, metricNameAttemptLatencies)
		if err != nil {
			return err
		}
		attemptLatAllAttrs := metric.WithAttributes(append(config.clientAttributes, attemptLatCurrCallAttrs...)...)

		// Attributes for server_latencies
		serverLatCurrCallAttrs, err := newOtelMetricAttrs(mt, metricNameServerLatencies)
		if err != nil {
			return err
		}
		serverLatAllAttres := metric.WithAttributes(append(config.clientAttributes, serverLatCurrCallAttrs...)...)

		for _, builtInMetricInstruments := range config.instruments {
			builtInMetricInstruments.distributions[metricNameAttemptLatencies].Record(ctx, float64(elapsedTime), attemptLatAllAttrs)

			serverLatency, err := getServerLatency(mt.headerTrailerMD)
			if err != nil {
				return err
			}
			builtInMetricInstruments.distributions[metricNameServerLatencies].Record(ctx, float64(serverLatency), serverLatAllAttres)
		}
		return err
	}
}

// recordOperationCompletion returns a function that should be executed to record total operation metrics
func (config *metricsConfigInternal) recordOperationCompletion(ctx context.Context, mt *builtinMetricsTracer) func() error {
	if !config.builtInEnabled {
		return noOpRecordFn
	}
	startTime := time.Now()

	return func() error {
		// Calculate elapsed time
		elapsedTime := time.Since(startTime).Nanoseconds()

		// Attributes for operation_latencies
		opLatCurrCallAttrs, err := newOtelMetricAttrs(mt, metricNameOperationLatencies)
		if err != nil {
			return err
		}
		opLatAllAttrs := metric.WithAttributes(append(config.clientAttributes, opLatCurrCallAttrs...)...)

		// Attributes for retry_count
		retryCntCurrCallAttrs, err := newOtelMetricAttrs(mt, metricNameRetryCount)
		if err != nil {
			return err
		}
		retryCntAllAttrs := metric.WithAttributes(append(config.clientAttributes, retryCntCurrCallAttrs...)...)

		for _, builtInMetricInstruments := range config.instruments {
			builtInMetricInstruments.distributions[metricNameOperationLatencies].Record(ctx, float64(elapsedTime), opLatAllAttrs)

			// Only record when retry count is greater than 0 so the retry
			// graph will be less confusing
			if mt.attemptCount > 1 {
				builtInMetricInstruments.counters[metricNameRetryCount].Add(ctx, mt.attemptCount-1, retryCntAllAttrs)
			}
		}
		return err
	}
}

// newOtelMetricAttrs converts recorded metric attributes values to OpenTelemetry attributes format
func newOtelMetricAttrs(metricValues *builtinMetricsTracer, metricName string) ([]attribute.KeyValue, error) {
	clusterID, zoneID, err := getLocation(metricValues.headerTrailerMD)
	if err != nil {
		return nil, err
	}

	// Create attribute key value pairs for attributes common to all metricss
	attrKeyValues := []attribute.KeyValue{
		attribute.String(metricLabelKeyAppProfile, metricValues.appProfileID),
		attribute.String(metricLabelKeyMethod, metricValues.method),

		// Add resource labels to otel metric labels.
		// These will be used for creating the monitored resource but exporter
		// will not add them to Google Cloud Monitoring metric labels
		attribute.String(monitoredResLabelKeyTable, metricValues.tableName),
		attribute.String(monitoredResLabelKeyCluster, clusterID),
		attribute.String(monitoredResLabelKeyZone, zoneID),
	}

	metricDetails, found := builtinMetrics[metricName]
	if !found {
		return nil, fmt.Errorf("Unable to create attributes list for unknown metric: %v", metricName)
	}

	// Add additional attributes to metrics
	for _, attrKey := range metricDetails.additionalAttributes {
		switch attrKey {
		case metricLabelKeyOperationStatus:
			attrKeyValues = append(attrKeyValues, attribute.String(metricLabelKeyOperationStatus, metricValues.status))
		case metricLabelKeyStreamingOperation:
			attrKeyValues = append(attrKeyValues, attribute.Bool(metricLabelKeyStreamingOperation, metricValues.isStreaming))
		default:
			return nil, fmt.Errorf("Unknown additional attribute: %v", attrKey)
		}
	}

	return attrKeyValues, err
}

// get GFE latency from response metadata
func getServerLatency(grpcCallOptions []grpc.CallOption) (int, error) {
	serverLatency := 0
	serverTimingKey := "server-timing"
	var err error

	headerOption, trailerOption, err := getHeaderTrailer(grpcCallOptions)
	if err != nil {
		return serverLatency, err
	}

	serverTimingStr := ""

	// Check whether server latency available in response header metadata
	headerMD := headerOption.HeaderAddr
	if headerMD != nil {
		headerMDValues := headerMD.Get(serverTimingKey)
		if len(headerMDValues) != 0 {
			serverTimingStr = headerMDValues[0]
		}
	}

	if len(serverTimingStr) == 0 {
		// Check whether server latency available in response trailer metadata
		trailerMD := trailerOption.TrailerAddr
		if trailerMD != nil {
			trailerMDValues := trailerMD.Get(serverTimingKey)
			if len(trailerMDValues) != 0 {
				serverTimingStr = trailerMDValues[0]
			}
		}
	}

	serverTimingValPrefix := "gfet4t7; dur="
	serverLatency, err = strconv.Atoi(strings.TrimPrefix(serverTimingStr, serverTimingValPrefix))
	if !strings.HasPrefix(serverTimingStr, serverTimingValPrefix) || err != nil {
		return serverLatency, err
	}

	return serverLatency, nil
}

// Obtain cluster and zone from response metadata
func getLocation(grpcCallOptions []grpc.CallOption) (string, string, error) {
	return "instance01-c1", "us-west2-a", nil
}

func getLocation2(grpcCallOptions []grpc.CallOption) (string, string, error) {
	var locationMetadata []string

	// Check whether location metadata available in response header metadata
	headerOption, trailerOption, err := getHeaderTrailer(grpcCallOptions)
	if err != nil {
		return "", "", err
	}
	headerMD := headerOption.HeaderAddr
	if headerMD != nil {
		locationMetadata = headerMD.Get(locationMetadataKey)
	}

	if locationMetadata == nil {
		// Check whether location metadata available in response trailer metadata
		// if none found in response header metadata
		trailerMD := trailerOption.TrailerAddr
		if trailerMD != nil {
			locationMetadata = trailerMD.Get(locationMetadataKey)
		}
	}

	if len(locationMetadata) < 1 {
		return "", "", fmt.Errorf("Failed to get location metadata")
	}

	// Unmarshal binary location metadata
	responseParams := &btpb.ResponseParams{}
	err = proto.Unmarshal([]byte(locationMetadata[0]), responseParams)
	if err != nil {
		return "", "", err
	}

	return responseParams.GetClusterId(), responseParams.GetZoneId(), nil
}

// Converts call options to HeaderCallOption and TrailerCallOption
func getHeaderTrailer(grpcCallOptions []grpc.CallOption) (grpc.HeaderCallOption, grpc.TrailerCallOption, error) {
	var headerOption grpc.HeaderCallOption
	var trailerOption grpc.TrailerCallOption

	if len(grpcCallOptions) != 2 {
		return headerOption, trailerOption, fmt.Errorf("Missing header trailer call options")
	}

	headerOption, isHeaderCallOption := grpcCallOptions[0].(grpc.HeaderCallOption)
	if !isHeaderCallOption {
		return headerOption, trailerOption, fmt.Errorf("Unknown call option")
	}

	trailerOption, isTrailerCallOption := grpcCallOptions[1].(grpc.TrailerCallOption)
	if !isTrailerCallOption {
		return headerOption, trailerOption, fmt.Errorf("Unknown call option")
	}

	return headerOption, trailerOption, nil
}
