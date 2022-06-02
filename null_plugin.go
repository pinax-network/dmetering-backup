package dmetering

import (
	"context"
	"github.com/pinax-network/dtypes/authentication"
	"github.com/pinax-network/dtypes/metering"
	"go.uber.org/atomic"
)

func init() {
	Register("null", func(config string) (Metering, error) {
		return newNullPlugin(), nil
	})
}

type nullPlugin struct {
	messagesCount atomic.Uint64
}

func newNullPlugin() *nullPlugin {
	return &nullPlugin{}
}

func (p *nullPlugin) EmitWithContext(ev metering.Event, ctx context.Context) {
	p.messagesCount.Inc()
}

func (p *nullPlugin) EmitWithCredentials(ev metering.Event, creds authentication.Credentials) {
	p.messagesCount.Inc()
}

func (p *nullPlugin) GetStatusCounters() (total, errors uint64) {
	return p.messagesCount.Load(), 0
}

func (p *nullPlugin) WaitToFlush() {
}
