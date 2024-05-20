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
	"time"

	"cloud.google.com/go/bigtable/internal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/sdk/resource"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

const (
	metricsPrefix       = "bigtable/"
	locationMetadataKey = "x-goog-ext-425905942-bin"

	// Monitored resource labels
	monitoredResLabelKeyProject  = "project_id"
	monitoredResLabelKeyInstance = "instance"
	monitoredResLabelKeyCluster  = "cluster"
	monitoredResLabelKeyTable    = "table"
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
)

var (
	clientName = fmt.Sprintf("cloud.google.com/go/bigtable v%v", internal.Version)
)

// Generates unique client ID in the format go-<random UUID>@<>hostname
func generateClientUID() string {
	hostname := "localhost"
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println(err)
	}
	return "go-" + uuid.NewString() + "@" + hostname

}

func createMetricsConfig(ctx context.Context, project, instance string) (*metricsConfig, error) {
	config := &metricsConfig{
		project:          project,
		commonAttributes: []attribute.KeyValue{},
	}

	// Construct attributes for metrics
	attributeMap := []attribute.KeyValue{
		attribute.String(monitoredResLabelKeyProject, project),
		attribute.String(monitoredResLabelKeyInstance, instance),
		attribute.String(metricLabelKeyClientUID, generateClientUID()),
		attribute.String(metricLabelKeyClientName, clientName),
	}
	config.commonAttributes = append(config.commonAttributes, attributeMap...)

	err := setOpenTelemetryMetricProvider(ctx, config)
	if err != nil {
		return config, err
	}

	return config, nil
}

func setOpenTelemetryMetricProvider(ctx context.Context, config *metricsConfig) error {
	// Fallback to default meter provider if none provided
	defaultExporter, err := newMonitoringExporter(ctx, config)
	if err != nil {
		return err
	}

	res, err := resource.New(ctx)
	if err != nil {
		return err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(defaultExporter.exporter)),
		sdkmetric.WithResource(res),
	)
	config.meterProvider = mp
	return config.initializeMetricInstruments()
}

func (config *metricsConfig) initializeMetricInstruments() error {
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

	return nil
}

type metricLabelValues struct {
	tableName       string
	appProfileId    string
	methodName      string
	isStreaming     bool
	status          *string
	grpcCallOptions []grpc.CallOption // Contains the header and trailer response metadata which is used to extract cluster and zone
}

func newMetricLabelValues(methodName, tableName, appProfile string, isStreaming bool) metricLabelValues {
	headerMD := metadata.New(nil)
	trailerMD := metadata.New(nil)
	status := ""
	grpcCallOptions := []grpc.CallOption{grpc.Header(&headerMD), grpc.Trailer(&trailerMD)}

	return metricLabelValues{
		tableName:       tableName,
		appProfileId:    appProfile,
		methodName:      methodName,
		isStreaming:     isStreaming,
		status:          &status,
		grpcCallOptions: grpcCallOptions,
	}
}

func (config *metricsConfig) recordOperationLatency(ctx context.Context, attr *metricLabelValues) func() error {
	startTime := time.Now()
	return func() error {
		elapsedTime := time.Since(startTime).Milliseconds()
		if config == nil || config.metricOperationLatencies == nil {
			return nil
		}

		attrMap, err := createAttributeMap(attr)
		config.metricOperationLatencies.Record(ctx, elapsedTime, metric.WithAttributes(append(config.commonAttributes, attrMap...)...))
		return err
	}
}

func createAttributeMap(attr *metricLabelValues) ([]attribute.KeyValue, error) {
	clusterId, zoneId, err := obtainLocationFromMetadata(attr.grpcCallOptions)
	if err != nil {
		return nil, err
	}
	attributeMap := []attribute.KeyValue{
		attribute.String(metricLabelKeyMethod, attr.methodName),
		attribute.String(metricLabelKeyAppProfile, attr.appProfileId),
		attribute.Bool(metricLabelKeyStreamingOperation, attr.isStreaming),
		attribute.String(metricLabelKeyOperationStatus, *attr.status),

		// Add resource labels to otel metric labels.
		// These will be used for creating the monitored resource but exporter will not add them to Cloud Monitoring metric labels
		attribute.String(monitoredResLabelKeyTable, attr.tableName),
		attribute.String(monitoredResLabelKeyCluster, clusterId),
		attribute.String(monitoredResLabelKeyZone, zoneId),
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
