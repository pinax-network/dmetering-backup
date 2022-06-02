package redis

import (
	"context"
	"encoding/hex"
	"github.com/streamingfast/dauth/authenticator"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/form3tech-oss/jwt-go"
	"github.com/golang/protobuf/proto"
	auth_redis "github.com/streamingfast/dauth/authenticator/redis"
	pbbilling "github.com/streamingfast/dauth/pb/dfuse/billing/v1"
	"github.com/streamingfast/dmetering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEmitWithContext(t *testing.T) {
	done := make(chan bool)

	topicProvider := func(pubsubProject string, t string) *pubsub.Topic {
		return nil
	}
	topicEmitter := func(event *pbbilling.Event) {
		assert.Equal(t, "subject.1", event.UserId)
		assert.Equal(t, "api.key.1", event.ApiKeyId)
		assert.Equal(t, "usage.1", event.Usage)
		close(done)
	}

	m := newMetering("network", "proj", "topic", false, 10*time.Millisecond, topicProvider, topicEmitter)

	ctx := context.Background()
	ctx = authenticator.WithCredentials(ctx, &auth_redis.Credentials{
		StandardClaims: jwt.StandardClaims{
			Subject: "subject.1",
		},
		APIKeyID: "api.key.1",
		Usage:    "usage.1",
	})
	m.EmitWithContext(dmetering.Event{}, ctx)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Time exceeded")
	}

}

func TestEmitWithContextMissingCredentials(t *testing.T) {
	done := make(chan bool)

	topicProvider := func(pubsubProject string, t string) *pubsub.Topic {
		return nil
	}
	topicEmitter := func(event *pbbilling.Event) {
		assert.Equal(t, "anonymous", event.UserId)
		assert.Equal(t, "anonymous", event.ApiKeyId)
		assert.Equal(t, "anonymous", event.Usage)
		close(done)
	}

	m := newMetering("network.1", "P", "dev-billable-events-v2", false, 10*time.Millisecond, topicProvider, topicEmitter)
	m.EmitWithContext(dmetering.Event{}, context.Background())

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Time exceeded")
	}

}

func TestEmitter(t *testing.T) {
	ctx := context.Background()
	c, _ := NewFakePubsub(ctx)
	topic, err := c.CreateTopic(context.Background(), "dev-billable-events-v2")
	require.NoError(t, err)

	sub, err := c.CreateSubscription(context.Background(), "dev-billable-events-v2", pubsub.SubscriptionConfig{Topic: topic})
	require.NoError(t, err)

	topicProvider := func(pubsubProject string, t string) *pubsub.Topic {
		return topic
	}

	m := newMetering("network.1", "P", "dev-billable-events-v2", false, 10*time.Millisecond, topicProvider, nil)

	done := make(chan bool)

	// Fake "bean-counter" subscriber
	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			zlog.Debug("received message", zap.String("data_hex", hex.EncodeToString(message.Data)))
			cmd := &pbbilling.Command{}
			err := proto.Unmarshal(message.Data, cmd)
			assert.NoError(t, err)
			assert.NotNil(t, cmd.GetEventAction().Event)
			event := cmd.GetEventAction().Event
			assert.Equal(t, "user.id.1", event.UserId)
			assert.Equal(t, "network.1", event.Network)
			assert.Equal(t, "source.1", event.Source)
			assert.Equal(t, "kind.1", event.Kind)
			assert.NotNil(t, event.Timestamp)

			mCount, eCount := m.GetStatusCounters()
			assert.Equal(t, uint64(1), mCount)
			assert.Equal(t, uint64(0), eCount)
			close(done)
		})
		require.NoError(t, err)
	}()

	m.EmitWithCredentials(dmetering.Event{
		Source: "source.1",
		Kind:   "kind.1",
	}, &auth_redis.Credentials{
		StandardClaims: jwt.StandardClaims{
			Subject: "user.id.1",
		},
	})

	m.WaitToFlush()
	zlog.Info("emitted, waiting")

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Time exceeded")
	}
}
