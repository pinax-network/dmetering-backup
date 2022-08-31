package redis

import (
	"github.com/pinax-network/dtypes/metering"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Accumulator struct {
	events       map[string]*metering.Event
	eventsLock   sync.Mutex
	emitter      topicEmitterFunc
	emitterDelay time.Duration
}

func newAccumulator(emitter func(event *metering.Event), emitterDelay time.Duration) *Accumulator {
	accumulator := &Accumulator{
		events:       make(map[string]*metering.Event),
		emitter:      emitter,
		emitterDelay: emitterDelay,
	}
	go accumulator.delayedEmitter()
	return accumulator
}

func (a *Accumulator) emit(event *metering.Event) {
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
	e.IngressBytes += event.IngressBytes
	e.EgressBytes += event.EgressBytes
	curTime := time.Now()
	e.Time = &curTime
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
	a.events = make(map[string]*metering.Event)
	a.eventsLock.Unlock()

	for _, event := range toSend {
		a.emitter(event)
	}
}

func eventToKey(event *metering.Event) string {
	return event.Source +
		event.Kind +
		event.Method +
		event.Network +
		event.UserId +
		event.ApiKeyId +
		event.IpAddress
}
