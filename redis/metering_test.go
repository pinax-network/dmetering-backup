package redis

import (
	"context"
	"encoding/hex"
	"github.com/golang-jwt/jwt/v4"
	"github.com/pinax-network/dtypes/authentication"
	"github.com/pinax-network/dtypes/metering"
	"github.com/pinax-network/dtypes/proto/v1/pb"
	"github.com/streamingfast/dauth/authenticator"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEmitWithContext(t *testing.T) {
	done := make(chan bool)

	topicEmitter := func(event *metering.Event) {
		assert.Equal(t, "subject.1", event.UserId)
		assert.Equal(t, "api.key.1", event.ApiKeyId)
		close(done)
	}

	m := newMetering("network", []string{"host1", "host2"}, "topic", false, 10*time.Millisecond, topicEmitter)

	ctx := context.Background()
	ctx = authenticator.WithCredentials(ctx, &authentication.JwtCredentials{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "subject.1",
		},
		ApiKeyId: "api.key.1",
	})
	m.EmitWithContext(metering.Event{}, ctx)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Time exceeded")
	}

}

func TestEmitWithContextMissingCredentials(t *testing.T) {
	done := make(chan bool)

	topicEmitter := func(event *metering.Event) {
		assert.Equal(t, "anonymous", event.UserId)
		assert.Equal(t, "anonymous", event.ApiKeyId)
		close(done)
	}

	m := newMetering("network.1", []string{"host1", "host2"}, "dev-billable-events-v2", false, 10*time.Millisecond, topicEmitter)
	m.EmitWithContext(metering.Event{}, context.Background())

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

	done := make(chan bool)
	topicEmitter := func(event *metering.Event) {
		assert.Equal(t, "anonymous", event.UserId)
		assert.Equal(t, "anonymous", event.ApiKeyId)
		close(done)
	}

	m := newMetering("network.1", []string{"host1", "host2"}, "dev-billable-events-v2", false, 10*time.Millisecond, topicEmitter)

	// Fake "bean-counter" subscriber
	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			zlog.Debug("received message", zap.String("data_hex", hex.EncodeToString(message.Data)))
			event := &pb.Event{}
			err := proto.Unmarshal(message.Data, event)
			assert.NoError(t, err)
			assert.NotNil(t, event)
			assert.Equal(t, "user.id.1", event.UserId)
			assert.Equal(t, "network.1", event.Network)
			assert.Equal(t, "source.1", event.Source)
			assert.Equal(t, "kind.1", event.Kind)
			assert.NotNil(t, event.Time)

			mCount, eCount := m.GetStatusCounters()
			assert.Equal(t, uint64(1), mCount)
			assert.Equal(t, uint64(0), eCount)
			close(done)
		})
		require.NoError(t, err)
	}()

	m.EmitWithCredentials(metering.Event{
		Source: "source.1",
		Kind:   "kind.1",
	}, &authentication.JwtCredentials{
		RegisteredClaims: jwt.RegisteredClaims{
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
