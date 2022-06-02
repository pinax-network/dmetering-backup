package dmetering

import (
	"context"
	"github.com/pinax-network/dtypes/authentication"
	"github.com/pinax-network/dtypes/metering"
)

var defaultMeter Metering = newNullPlugin()

func SetDefaultMeter(m Metering) {
	defaultMeter = m
}

func EmitWithContext(ev metering.Event, ctx context.Context) {
	defaultMeter.EmitWithContext(ev, ctx)
}

func EmitWithCredentials(ev metering.Event, creds authentication.Credentials) {
	defaultMeter.EmitWithCredentials(ev, creds)
}

func GetStatusCounters() (total, errors uint64) {
	return defaultMeter.GetStatusCounters()
}

func WaitToFlush() {
	defaultMeter.WaitToFlush()
}
