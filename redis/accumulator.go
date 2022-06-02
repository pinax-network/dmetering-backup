package redis

import (
	"github.com/streamingfast/dmetering"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"go.uber.org/zap"
)

type Accumulator struct {
	events       map[string]*dmetering.Event
	eventsLock   sync.Mutex
	emitter      topicEmitterFunc
	emitterDelay time.Duration
}

func newAccumulator(emitter func(event *dmetering.Event), emitterDelay time.Duration) *Accumulator {
	accumulator := &Accumulator{
		events:       make(map[string]*dmetering.Event),
		emitter:      emitter,
		emitterDelay: emitterDelay,
	}
	go accumulator.delayedEmitter()
	return accumulator
}

func (a *Accumulator) emit(event *dmetering.Event) {
	zlog.Debug("accumulator emitting", zap.String("user_id", event.UserId))
	a.eventsLock.Lock()
	defer a.eventsLock.Unlock()

	key := eventToKey(event)
	e := a.events[key]

	if e == nil {
		a.events[key] = event
		return
	}

	e.RequestsCount += event.RequestsCount
	e.ResponsesCount += event.ResponsesCount
	e.RateLimitHitCount += event.RateLimitHitCount
	e.IngressBytes += event.IngressBytes
	e.EgressBytes += event.EgressBytes
	e.IdleTime += event.IdleTime
	e.Timestamp = ptypes.TimestampNow()

}

func (a *Accumulator) delayedEmitter() {
	for {
		time.Sleep(a.emitterDelay)
		zlog.Debug("accumulator sleep over")
		a.emitAccumulatedEvents()
	}
}

func (a *Accumulator) emitAccumulatedEvents() {
	zlog.Debug("emitting accumulated events")
	a.eventsLock.Lock()
	toSend := a.events
	a.events = make(map[string]*dmetering.Event)
	a.eventsLock.Unlock()

	for _, event := range toSend {
		a.emitter(event)
	}
}

func eventToKey(event *dmetering.Event) string {
	//UserId               string
	//Kind                 string
	//Source               string
	//Network              string
	//Usage                string
	//ApiKeyId             string
	//IpAddress            string -- optional -- collected circa 2020-01-20
	//Method               string -- optional -- collected circa 2020-02-18

	// Accumulator `GROUP BY` key
	return event.UserId +
		event.Kind +
		event.Source +
		event.Network +
		event.Usage +
		event.ApiKeyId +
		event.IpAddress +
		event.Method
}
