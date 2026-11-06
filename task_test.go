package cloudwatchmetricer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

func TestTaskGetMetricData(t *testing.T) {
	timestamp := time.Date(2026, time.November, 6, 12, 0, 0, 0, time.UTC)
	ctx := t.Context()
	ta := &task{
		namespace: "test/metrics",
		metricer: Func(func(got context.Context) ([]*Metric, error) {
			if got != ctx {
				t.Fatal("collector received a different context")
			}
			return []*Metric{
				{
					Name:      "requests",
					Value:     42,
					Unit:      "Count",
					Timestamp: timestamp,
					Dimensions: map[string]string{
						"service": "api",
						"region":  "test-region",
					},
				},
				{Name: "latency", Value: 1.5, Unit: "Seconds"},
			}, nil
		}),
	}
	before := time.Now()
	inputs, err := ta.getMetricData(ctx)
	after := time.Now()
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 1 || len(inputs[0].MetricData) != 2 {
		t.Fatalf("unexpected metric batches: %v", inputs)
	}
	if got := aws.StringValue(inputs[0].Namespace); got != ta.namespace {
		t.Errorf("namespace = %q, want %q", got, ta.namespace)
	}
	explicit, implicit := inputs[0].MetricData[0], inputs[0].MetricData[1]
	if aws.StringValue(explicit.MetricName) != "requests" ||
		aws.Float64Value(explicit.Value) != 42 ||
		aws.StringValue(explicit.Unit) != "Count" ||
		!aws.TimeValue(explicit.Timestamp).Equal(timestamp) {
		t.Errorf("explicit metric = %v", explicit)
	}
	dimensions := make(map[string]string)
	for _, dimension := range explicit.Dimensions {
		dimensions[aws.StringValue(dimension.Name)] = aws.StringValue(dimension.Value)
	}
	if len(explicit.Dimensions) != 2 || dimensions["service"] != "api" || dimensions["region"] != "test-region" {
		t.Errorf("dimensions = %v", explicit.Dimensions)
	}
	gotTime := aws.TimeValue(implicit.Timestamp)
	if aws.StringValue(implicit.MetricName) != "latency" ||
		aws.Float64Value(implicit.Value) != 1.5 ||
		aws.StringValue(implicit.Unit) != "Seconds" ||
		gotTime.Before(before) || gotTime.After(after) ||
		len(implicit.Dimensions) != 0 {
		t.Errorf("implicit metric = %v", implicit)
	}
}

func TestTaskGetMetricDataBatches(t *testing.T) {
	for _, count := range []int{0, 1, 20, 21, 41} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			metrics := make([]*Metric, count)
			for i := range metrics {
				metrics[i] = &Metric{Name: fmt.Sprintf("metric-%d", i)}
			}
			ta := &task{
				namespace: "test/batches",
				metricer: Func(func(context.Context) ([]*Metric, error) {
					return metrics, nil
				}),
			}
			inputs, err := ta.getMetricData(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got, want := len(inputs), (count+19)/20; got != want {
				t.Fatalf("batch count = %d, want %d", got, want)
			}
			index := 0
			for _, input := range inputs {
				if got, want := len(input.MetricData), min(20, count-index); got != want {
					t.Fatalf("batch size = %d, want %d", got, want)
				}
				if aws.StringValue(input.Namespace) != ta.namespace {
					t.Errorf("batch namespace = %q, want %q", aws.StringValue(input.Namespace), ta.namespace)
				}
				for _, datum := range input.MetricData {
					if got, want := aws.StringValue(datum.MetricName), metrics[index].Name; got != want {
						t.Errorf("metric %d name = %q, want %q", index, got, want)
					}
					index++
				}
			}
		})
	}
}
