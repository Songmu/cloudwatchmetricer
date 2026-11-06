package cloudwatchmetricer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

type mockCloudWatch struct {
	cloudwatchiface.CloudWatchAPI
	putMetricData func(aws.Context, *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error)
}

func (m *mockCloudWatch) PutMetricDataWithContext(ctx aws.Context, input *cloudwatch.PutMetricDataInput, _ ...request.Option) (*cloudwatch.PutMetricDataOutput, error) {
	return m.putMetricData(ctx, input)
}

func TestBrokerPutMetrics(t *testing.T) {
	collectorErr := errors.New("collector failed")
	apiErr := errors.New("API failed")
	for _, tt := range []struct {
		name         string
		collectorErr error
		apiErr       error
		wantCalls    int
		wantErr      error
		wantMessage  string
	}{
		{name: "success", wantCalls: 2},
		{name: "collection error", collectorErr: collectorErr, wantErr: collectorErr, wantMessage: "failed to getMetricData"},
		{name: "submission error continues to next batch", apiErr: apiErr, wantCalls: 2, wantErr: apiErr, wantMessage: "failed to PutMetricData"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			calls := 0
			var handled []error
			broker := New(&mockCloudWatch{
				putMetricData: func(got aws.Context, input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
					if got != ctx {
						t.Error("API received a different context")
					}
					if aws.StringValue(input.Namespace) != "test/broker" {
						t.Errorf("namespace = %q", aws.StringValue(input.Namespace))
					}
					calls++
					if calls == 1 && tt.apiErr != nil {
						return nil, tt.apiErr
					}
					return &cloudwatch.PutMetricDataOutput{}, nil
				},
			})
			broker.ErrorHandler = func(err error) {
				handled = append(handled, err)
			}
			broker.putMetrics(ctx, &task{
				name:      "test-task",
				namespace: "test/broker",
				metricer: Func(func(context.Context) ([]*Metric, error) {
					metrics := make([]*Metric, 21)
					for i := range metrics {
						metrics[i] = &Metric{Name: "requests", Value: float64(i)}
					}
					return metrics, tt.collectorErr
				}),
			})
			if calls != tt.wantCalls {
				t.Errorf("API calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantErr == nil {
				if len(handled) != 0 {
					t.Errorf("unexpected errors: %v", handled)
				}
				return
			}
			if len(handled) != 1 {
				t.Fatalf("handled errors = %v, want one", handled)
			}
			if !errors.Is(handled[0], tt.wantErr) ||
				!strings.Contains(handled[0].Error(), tt.wantMessage) ||
				!strings.Contains(handled[0].Error(), "test-task") {
				t.Errorf("error = %v, want wrapped %v with operation and task name", handled[0], tt.wantErr)
			}
		})
	}
}

func TestBrokerRejectsRegistrationAfterRun(t *testing.T) {
	broker := New(nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := broker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := broker.Register("late-task", "test/broker", 0, nil); err == nil {
		t.Fatal("Register succeeded after Run returned")
	}
}
