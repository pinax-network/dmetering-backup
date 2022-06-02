package redis

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/pinax-network/dtypes/authentication"
	"github.com/pinax-network/dtypes/metering"
	"github.com/streamingfast/dauth/authenticator"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/dmetering"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func init() {
	dmetering.Register("redis", func(config string) (dmetering.Metering, error) {
		u, err := url.Parse(config)
		if err != nil {
			return nil, err
		}

		vals := u.Query()
		networkID := vals.Get("networkId")
		if networkID == "" {
			return nil, fmt.Errorf("missing networkId query param to metering config")
		}

		var emitterDelay = 10 * time.Second
		emitterDelayString := vals.Get("emitterDelay")
		if emitterDelayString != "" {
			if d, err := time.ParseDuration(emitterDelayString); err == nil {
				emitterDelay = d
			}
		}

		project := u.Host
		if project == "" {
			return nil, fmt.Errorf("project not specified (as hostname)")
		}

		hosts := strings.Split(u.Host, ",")

		topic := strings.TrimLeft(u.Path, "/")
		if topic == "" {
			return nil, fmt.Errorf("topic not specified (as path component)")
		}

		dbString := vals.Get("redisDB")
		db := 0
		if dbString != "" {
			db, err = strconv.Atoi(dbString)

			if err != nil {
				err = fmt.Errorf("failed to parse redisDB parameter, not an integer: %s", dbString)
				return nil, err
			}
		}

		warnOnErrors := vals.Get("warnOnErrors") == "true"

		return newMetering(networkID, hosts, db, topic, warnOnErrors, emitterDelay, nil), nil
	})
}

type meteringPlugin struct {
	network string

	redisClient        *redis.Client
	warnOnPubSubErrors bool
	pubSubTopic        string

	messagesCount atomic.Uint64
	errorCount    atomic.Uint64

	accumulator *Accumulator
}

// type topicProviderFunc func(pubsubProject string, topicName string) *pubsub.Topic
type topicEmitterFunc func(e *metering.Event)

func newMetering(network string, hosts []string, db int, pubSubTopic string, warnOnPubSubErrors bool, emitterDelay time.Duration /*topicProvider topicProviderFunc,*/, topicEmitter topicEmitterFunc) *meteringPlugin {
	m := &meteringPlugin{
		network:            network,
		warnOnPubSubErrors: warnOnPubSubErrors,
	}

	m.redisClient = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: hosts,
		DB:            db,
	})

	m.pubSubTopic = pubSubTopic

	/*	if topicProvider == nil {
			m.topic = defaultTopicProvider(pubSubProject, pubSubTopic)
		} else {
			m.topic = topicProvider(pubSubProject, pubSubTopic)
		}*/

	if topicEmitter == nil {
		m.accumulator = newAccumulator(m.defaultTopicEmitter, emitterDelay)
	} else {
		m.accumulator = newAccumulator(topicEmitter, emitterDelay)
	}

	zlog.Info("metering is ready to emit")
	return m
}

func (m *meteringPlugin) EmitWithContext(ev metering.Event, ctx context.Context) {
	credentials := authenticator.GetCredentials(ctx)
	m.EmitWithCredentials(ev, credentials)
}

func (m *meteringPlugin) EmitWithCredentials(ev metering.Event, creds authentication.Credentials) {

	switch c := creds.(type) {
	case *authentication.JwtCredentials:
		ev.UserId = c.Subject
		ev.ApiKeyId = c.ApiKeyId
		// userEvent.Usage = c.Usage
		ev.IpAddress = c.IP
	default:
		zlog.Warn("got invalid credentials type", zap.Any("c", c))
	}
	zlog.Debug("emit event", zap.Any("event", ev), zap.Any("credentials", creds))

	m.emit(ev)
}

func (m *meteringPlugin) emit(e metering.Event) {
	m.messagesCount.Inc()
	if e.Time == nil {
		curTime := time.Now()
		e.Time = &curTime
	}
	m.accumulator.emit(&e)
}

func (m *meteringPlugin) GetStatusCounters() (total, errors uint64) {
	return m.messagesCount.Load(), m.errorCount.Load()
}

func (m *meteringPlugin) WaitToFlush() {
	zlog.Info("gracefully shutting down, now flushing pending dbilling events")
	m.accumulator.emitAccumulatedEvents()
	// m.topic.Stop()
	zlog.Info("all billing events have been flushed before shutdown")
}

/*func defaultTopicProvider(pubsubProject string, topicName string) *pubsub.Topic {
	ctx := context.Background()

	client := redis.NewClusterClient(&redis.ClusterOptions{

	})

	topics, err := client.PubSubChannels(ctx, topicName).Result()

	if err != nil || len(topics) == 0 {
		zlog.Panic("unable to setup dbilling PubSub connection", zap.String("project", pubsubProject), zap.String("topic", topicName), zap.Error(err))
	}

	client.Publish(ctx)

	topic := client.Topic(topicName)
	topic.PublishSettings = pubsub.PublishSettings{
		ByteThreshold:  20000,
		CountThreshold: 100,
		DelayThreshold: 1 * time.Second,
	}

	exists, err := topic.Exists(ctx)
	if err != nil || !exists {
		zlog.Panic("unable to setup dbilling PubSub connection", zap.String("project", pubsubProject), zap.String("topic", topicName), zap.Error(err))
	}
	return topic
}*/

func (m *meteringPlugin) defaultTopicEmitter(e *metering.Event) {
	if e.UserId == "" || e.Source == "" || e.Kind == "" {
		zlog.Warn("events SHALL minimally contain UserID, Source and Kind, dropping billing event", zap.Any("event", e))
		return
	}

	data, err := proto.Marshal(e.ToProtobuf())
	if err != nil {
		m.errorCount.Inc()
		return
	}

	zlog.Debug("sending message", zap.String("data_hex", hex.EncodeToString(data)))
	res := m.redisClient.Publish(context.Background(), m.pubSubTopic, data)

	if m.warnOnPubSubErrors {
		if err := res.Err(); err != nil {
			zlog.Warn("failed to publish", zap.Error(err))
		}
	}
}
