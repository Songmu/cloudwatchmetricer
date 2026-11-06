package cloudwatchmetricer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

var defaultBroker = sync.OnceValues(newDefaultBroker)

func newDefaultBroker() (*Broker, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}
	return New(cloudwatch.NewFromConfig(cfg)), nil
}

// DefaultBroker returns the shared broker and panics if AWS configuration cannot be loaded.
func DefaultBroker() *Broker {
	broker, err := defaultBroker()
	if err != nil {
		panic(err)
	}
	return broker
}

func Run(ctx context.Context) error {
	broker, err := defaultBroker()
	if err != nil {
		return err
	}
	return broker.Run(ctx)
}

func Register(name, namespace string, interval time.Duration, me Metricer) error {
	broker, err := defaultBroker()
	if err != nil {
		return err
	}
	return broker.Register(name, namespace, interval, me)
}
