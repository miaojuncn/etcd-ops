package zlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultLogLevel = zapcore.InfoLevel
)

var Logger *zap.SugaredLogger

func init() {
	Logger, _ = NewDefaultSugarLogger(DefaultLogLevel)
}

func NewDefaultSugarLogger(level zapcore.Level) (*zap.SugaredLogger, error) {
	logger, err := NewDefaultLogger(level)
	if err != nil {
		return nil, err
	}
	return logger.Sugar(), nil
}

func NewDefaultLogger(level zapcore.Level) (*zap.Logger, error) {
	conf := DefaultZapLoggerConfig
	conf.Level = zap.NewAtomicLevelAt(level)
	c, err := conf.Build()
	if err != nil {
		return nil, err
	}
	return c, nil
}

var DefaultZapLoggerConfig = zap.Config{
	Level:       zap.NewAtomicLevelAt(DefaultLogLevel),
	Development: false,
	Sampling: &zap.SamplingConfig{
		Initial:    100,
		Thereafter: 100,
	},
	Encoding:         "json",
	OutputPaths:      []string{"stderr"},
	ErrorOutputPaths: []string{"stderr"},

	EncoderConfig: zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.RFC3339TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	},
}
