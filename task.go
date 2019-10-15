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
		nextInterval := ta.interval - (now.Sub(next) % ta.interval)
		next = now.Add(nextInterval)
		select {
		case <-ctx.Done():
			return
		case <-until(next):
			ch <- ta
		}
	}
}

func (ta *task) getMetricData(ctx context.Context) (*cloudwatch.PutMetricDataInput, error) {
	// XXX needs mutex?
	metrics, err := ta.metricer.Metrics(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	params := &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(ta.namespace),
	}
	for _, me := range metrics {
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
	return params, nil
}
