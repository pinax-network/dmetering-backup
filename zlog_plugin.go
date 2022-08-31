package dmetering

import (
	"context"
	"fmt"
	"github.com/pinax-network/dtypes/authentication"
	"github.com/pinax-network/dtypes/metering"
	"net/url"
	"path"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var defaultPackageID = "github.com/streamingfast/dmetering"

func init() {
	Register("zlog", func(dsn string) (Metering, error) {
		return newZlogPlugin(dsn)
	})
}

type zlogPlugin struct {
	total  atomic.Uint64
	level  zapcore.Level
	logger *zap.Logger
}

func newZlogPlugin(dsn string) (*zlogPlugin, error) {
	packageID, level, err := parseZlogPluginDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn %q: %w", dsn, err)
	}

	logger := zlog
	if packageID != defaultPackageID {
		// This cannot work unless we check if the logger is already registered. This method
		// is called multiple time, so registering multiple times cause a panic. Until we have
		// the ability to check if a package is already registered, we cannot enable the feature,
		// so it's disabled for now.
		// logger = zap.NewNop()
		// logging.Register(packageID, &logger)
	}

	return &zlogPlugin{total: atomic.Uint64{}, level: level, logger: logger}, nil
}

func (p *zlogPlugin) EmitWithContext(ev metering.Event, ctx context.Context) {
	p.total.Inc()

	p.logger.Check(p.level, "emitting event").Write(zap.Reflect("event", ev))
}

func (p *zlogPlugin) EmitWithCredentials(ev metering.Event, creds authentication.Credentials) {
	p.total.Inc()

	fields := append([]zap.Field{zap.Reflect("event", ev)}, creds.GetLogFields()...)
	p.logger.Check(p.level, "emitting event (with credentials)").Write(fields...)
}

func (p *zlogPlugin) GetStatusCounters() (total, errors uint64) {
	return p.total.Load(), 0
}

func (p *zlogPlugin) WaitToFlush() {
}

func parseZlogPluginDSN(dsn string) (packageID string, level zapcore.Level, err error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return packageID, level, fmt.Errorf("parse: %w", err)
	}

	packageID = path.Join(url.Host, url.Path)
	if packageID == "" {
		packageID = defaultPackageID
	}

	return packageID, zapPluginLevelFromQuery(url.Query()), nil
}

func zapPluginLevelFromQuery(query url.Values) zapcore.Level {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(query.Get("level"))); err != nil {
		zlog.Warn("unable to extract level from query", zap.String("value", query.Get("level")), zap.Error(err))
		return zap.InfoLevel
	}

	return level
}
