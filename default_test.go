package cloudwatchmetricer

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

func TestDefaultBroker(t *testing.T) {
	original := defaultBroker
	t.Cleanup(func() { defaultBroker = original })
	defaultBroker = sync.OnceValues(newDefaultBroker)

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	t.Setenv("AWS_CONFIG_FILE", configPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	t.Run("loads configuration and reuses broker", func(t *testing.T) {
		broker := DefaultBroker()
		if broker != DefaultBroker() {
			t.Error("DefaultBroker returned a different broker")
		}
		client, ok := broker.svc.(*cloudwatch.Client)
		if !ok {
			t.Fatalf("client type = %T, want *cloudwatch.Client", broker.svc)
		}
		if got := client.Options().Region; got != "us-west-2" {
			t.Errorf("region = %q, want us-west-2", got)
		}
	})

	t.Run("reports missing profile", func(t *testing.T) {
		t.Setenv("AWS_PROFILE", "cloudwatchmetricer-missing-profile")
		defaultBroker = sync.OnceValues(newDefaultBroker)
		broker, err := defaultBroker()
		if broker != nil || err == nil || !strings.Contains(err.Error(), "failed to load AWS configuration:") {
			t.Fatalf("initialization = (%v, %v), want nil broker and AWS configuration error", broker, err)
		}
		if got := Register("test", "test/metrics", 0, nil); got != err {
			t.Errorf("Register error = %v, want cached %v", got, err)
		}
		if got := Run(t.Context()); got != err {
			t.Errorf("Run error = %v, want cached %v", got, err)
		}
		defer func() {
			got := recover()
			if got != err {
				t.Errorf("panic = %v, want cached %v", got, err)
			}
		}()
		DefaultBroker()
	})
}
