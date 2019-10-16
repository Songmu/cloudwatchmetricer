package cloudwatchmetricer

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

func until(ti time.Time) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for {
			nextTi := <-time.After(time.Until(ti))
			if !nextTi.Before(ti) {
				close(ch)
				return
			}
		}
	}()
	return ch
}

type task struct {
	name, namespace string
	metricer        Metricer
	interval        time.Duration
}

func (ta *task) start(ctx context.Context, ch chan *task) {
	next := time.Now()
	for {
		now := time.Now()
		sub := now.Sub(next)
		times := (sub / ta.interval) + 1

		next = next.Add(ta.interval * times)
		select {
		case <-ctx.Done():
			return
		case <-until(next):
			ch <- ta
		}
	}
}

func (ta *task) getMetricData(ctx context.Context) ([]*cloudwatch.PutMetricDataInput, error) {
	// XXX needs mutex?
	metrics, err := ta.metricer.Metrics(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var ret []*cloudwatch.PutMetricDataInput

	length := len(metrics)
	var start, end int
	for end < length {
		// Each request is also limited to no more than 20 different metrics.
		// ref. https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
		end = start + 20
		if end > length {
			end = length
		}
		params := &cloudwatch.PutMetricDataInput{
			Namespace: aws.String(ta.namespace),
		}
		for _, me := range metrics[start:end] {
			var dim []*cloudwatch.Dimension
			if me.Dimensions != nil {
				for k, v := range me.Dimensions {
					dim = append(dim, &cloudwatch.Dimension{
						Name:  aws.String(k),
						Value: aws.String(v),
					})
				}
			}
			ti := me.Timestamp
			if ti.IsZero() {
				ti = now
			}
			params.MetricData = append(params.MetricData, &cloudwatch.MetricDatum{
				MetricName: aws.String(me.Name),
				Value:      aws.Float64(me.Value),
				Unit:       aws.String(me.Unit),
				Timestamp:  aws.Time(ti),
				Dimensions: dim,
			})
		}
		ret = append(ret, params)
		start = end
	}
	return ret, nil
}
