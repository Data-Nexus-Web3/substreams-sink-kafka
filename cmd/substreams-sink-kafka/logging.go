package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var zlog, tracer = logging.RootLogger("sink-kafka", "github.com/streamingfast/substreams-sink-kafka/cmd/substreams-sink-kafka")

func init() {
	cli.SetLogger(zlog, tracer)

	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))
}
