package dmetering

import (
	"context"
	"fmt"
	"github.com/pinax-network/dtypes/authentication"
	"github.com/pinax-network/dtypes/metering"
	"net/url"
)

type Metering interface {
	EmitWithContext(ev metering.Event, ctx context.Context)
	EmitWithCredentials(ev metering.Event, creds authentication.Credentials)
	GetStatusCounters() (total, errors uint64)
	WaitToFlush()
}

var registry = make(map[string]FactoryFunc)

func New(config string) (Metering, error) {
	u, err := url.Parse(config)
	if err != nil {
		return nil, err
	}

	factory := registry[u.Scheme]
	if factory == nil {
		panic(fmt.Sprintf("no Metering plugin named \"%s\" is currently registered", u.Scheme))
	}
	return factory(config)
}

type FactoryFunc func(config string) (Metering, error)

func Register(name string, factory FactoryFunc) {
	registry[name] = factory
}
