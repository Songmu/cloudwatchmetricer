package cloudwatchmetricer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

var defaultBroker = sync.OnceValue(newDefaultBroker)

func newDefaultBroker() *Broker {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to load AWS configuration: %w", err))
	}
	return New(cloudwatch.NewFromConfig(cfg))
}

// DefaultBroker returns the shared broker and panics if AWS configuration cannot be loaded.
func DefaultBroker() *Broker {
	return defaultBroker()
}

func Run(ctx context.Context) error {
	return DefaultBroker().Run(ctx)
}

func Register(name, namespace string, interval time.Duration, me Metricer) error {
	return DefaultBroker().Register(name, namespace, interval, me)
}
