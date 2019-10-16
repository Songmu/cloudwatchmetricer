package cloudwatchmetricer

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

var (
	brInit        sync.Once
	defaultBroker *Broker
)

func DefaultBroker() *Broker {
	brInit.Do(func() {
		defaultBroker = New(cloudwatch.New(session.New()))
	})
	return defaultBroker
}

func Run(ctx context.Context) error {
	return DefaultBroker().Run(ctx)
}

func Register(name, namespace string, interval time.Duration, me Metricer) error {
	return DefaultBroker().Register(name, namespace, interval, me)
}
