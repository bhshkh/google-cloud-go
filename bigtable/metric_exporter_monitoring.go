package bigtable

import (
	"fmt"
	"strings"

	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
)

const (
	meterName            = "bigtable.googleapis.com/internal/client/"
	bigtableResourceType = "bigtable_client_raw"
)

var (
	monitoredResLabelsSet = map[string]bool{
		monitoredResLabelKeyProject:  true,
		monitoredResLabelKeyInstance: true,
		monitoredResLabelKeyCluster:  true,
		monitoredResLabelKeyTable:    true,
		monitoredResLabelKeyZone:     true,
	}
)

type monitoringExporter struct {
	exporter sdkmetric.Exporter
}

func sanitizeUTF8(s string) string {
	return strings.ToValidUTF8(s, "�")
}

func newMonitoringMetricExporter(config *metricsConfig) (*monitoringExporter, error) {

	resourceOnlyKeys := []attribute.Key{}
	for k, _ := range monitoredResLabelsSet {
		resourceOnlyKeys = append(resourceOnlyKeys, attribute.Key(k))
	}

	exporter, err := mexporter.New(
		mexporter.WithProjectID(config.project),
		mexporter.WithMetricDescriptorTypeFormatter(func(desc metricdata.Metrics) string {
			return fmt.Sprintf("%v%s", meterName, desc.Name)
		}),
		// Do not add any Otel resource attributes to metric
		mexporter.WithFilteredResourceAttributes(mexporter.NoAttributes),

		// Do not create metric descriptor 'bigtable.googleapis.com/internal/client/' since it already exists
		mexporter.WithDisableCreateMetricDescriptors(),

		// Create monitored resource with type 'bigtable_client_raw'
		// Add attributes from the metric to monitored resource labels
		mexporter.WithMonitoredResourceCreator(getMonitoredResCreator()),

		// Do not add monitored resource labels to metric labels
		mexporter.WithFilteredMetricAttributes(attribute.NewDenyKeysFilter(resourceOnlyKeys...)),
	)
	if err != nil {
		return nil, err
	}
	return &monitoringExporter{
		exporter: exporter,
	}, nil
}

func getMonitoredResCreator() func(rm *metricdata.ResourceMetrics) (*monitoredres.MonitoredResource, error) {
	return func(rm *metricdata.ResourceMetrics) (*monitoredrespb.MonitoredResource, error) {
		mr := &monitoredrespb.MonitoredResource{
			Type:   bigtableResourceType,
			Labels: map[string]string{},
		}

		// Each metric datapoint has metric labels and monitored resource labels
		// Copy the metric labels to monitored resource labels
		for _, scope := range rm.ScopeMetrics {
			for _, metrics := range scope.Metrics {
				if metrics.Data == nil {
					return mr, nil
				}
				switch a := metrics.Data.(type) {
				case metricdata.Histogram[int64]:
					// Every datapoint has all the resource labels.
					// So, this loop will exit after first iteration
					for _, point := range a.DataPoints {
						attrIter := point.Attributes.Iter()
						for attrIter.Next() {
							kv := attrIter.Attribute()
							labelKey := string(kv.Key)
							if _, isResLabel := monitoredResLabelsSet[labelKey]; isResLabel {
								mr.Labels[labelKey] = sanitizeUTF8(kv.Value.Emit())

								// Check if all required labels added to monitored resource
								if len(mr.Labels) == len(monitoredResLabelsSet) {
									return mr, nil
								}
							}
						}
					}
				default:
					return mr, fmt.Errorf("bigtable: Unsupported metric type")
				}
			}
		}

		return mr, nil
	}
}
