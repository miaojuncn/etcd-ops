package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	level = zapcore.InfoLevel
)

func NewLogger() *zap.Logger {
	var encoder zapcore.Encoder

	encoder = zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:   "ts",
		LevelKey:  "level",
		NameKey:   "logger",
		CallerKey: "caller",
		//FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.RFC3339TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		//EncodeCaller: zapcore.FullCallerEncoder,
	})
	core := zapcore.NewCore(
		encoder,
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), // Print to console and file
		level,
	)
	//return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	return zap.New(core, zap.Development(), zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
}
