package cloudwatchmetricer

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

func TestDefaultBroker(t *testing.T) {
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
		defer func() {
			got := recover()
			if got == nil || !strings.Contains(fmt.Sprint(got), "failed to load AWS configuration:") {
				t.Errorf("panic = %v, want AWS configuration error", got)
			}
		}()
		newDefaultBroker()
	})
}
