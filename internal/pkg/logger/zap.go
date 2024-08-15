package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"time"
)

var Log *zap.SugaredLogger

func init() {
	Log = NewDefaultLogger()
}

func NewZapLogger(path, level string, maxSize, backups, age int, compress bool) *zap.SugaredLogger {
	return initLog(path, level, maxSize, backups, age, compress)
}

func NewDefaultLogger() *zap.SugaredLogger {
	return initLog("log.log", "debug", 10, 7, 7, false)
}

func AddFields(l *zap.SugaredLogger, fields ...zap.Field) *zap.SugaredLogger {
	for _, field := range fields {
		l = l.With(field)
	}
	return l
}

func initLog(path, level string, maxSize, backups, age int, compress bool) *zap.SugaredLogger {
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:   "msg",
		LevelKey:     "level",
		TimeKey:      "@timestamp",
		CallerKey:    "file",
		EncodeLevel:  zapcore.CapitalLevelEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			//enc.AppendString(t.Format("2006-01-02 15:04:05.999999"))
			enc.AppendString(t.Format(time.RFC3339Nano))
		},
		EncodeDuration: func(d time.Duration, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendInt64(int64(d) / 1000000)
		},
		ConsoleSeparator: " ",
	}

	logWriter := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    maxSize,
		MaxBackups: backups,
		MaxAge:     age,
		Compress:   compress,
	}

	l, err := zapcore.ParseLevel(level)
	if err != nil {
		fmt.Printf("fail to parse Log level, %s", err)
		l = zapcore.DebugLevel
	}

	levelEnabler := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= l
	})

	consoleEncoderConfig := encoderConfig
	consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	core := zapcore.NewTee(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.AddSync(logWriter),
			levelEnabler),
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(consoleEncoderConfig),
			zapcore.AddSync(os.Stdout),
			levelEnabler),
	)
	//log := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	sugarLog := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel)).Sugar()
	sugarLog.Sync()
	return sugarLog
}
