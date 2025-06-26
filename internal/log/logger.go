package log

import (
	"context"
	"fmt"
	"github.com/go-viper/mapstructure/v2"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
)

const (
	LevelTrace = slog.Level(-8)
	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
)

type Interface interface {
	Wrapped() *slog.Logger
	Handler() slog.Handler
	With(args ...any) Interface
	WithGroup(name string) Interface
	Enabled(ctx context.Context, level slog.Level) bool
	Log(ctx context.Context, level slog.Level, msg string, args ...any)
	Logf(ctx context.Context, level slog.Level, format string, args ...any)
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
	Trace(msg string, args ...any)
	Tracef(format string, args ...any)
	TraceContext(ctx context.Context, msg string, args ...any)
	Debug(msg string, args ...any)
	Debugf(format string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
	Info(msg string, args ...any)
	Infof(format string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	Warn(msg string, args ...any)
	Warnf(format string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	Error(msg string, args ...any)
	Errorf(format string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}

type wrappedLog struct {
	*slog.Logger
}

func (x *wrappedLog) Handler() slog.Handler {
	return x.Logger.Handler()
}

func (x *wrappedLog) Wrapped() *slog.Logger {
	return x.Logger
}

func (x *wrappedLog) With(args ...any) Interface {
	if len(args) == 0 {
		return x
	}

	filteredArgs := make([]any, 0, len(args))
	for _, arg := range args {
		if processed := filterStructFields(arg); processed != nil {
			filteredArgs = append(filteredArgs, processed)
		} else {
			filteredArgs = append(filteredArgs, arg)
		}
	}

	n := &wrappedLog{
		Logger: x.Logger.With(filteredArgs...),
	}
	return n
}

func (x *wrappedLog) WithGroup(name string) Interface {
	if name == "" {
		return x
	}

	n := &wrappedLog{
		Logger: x.Logger.WithGroup(name),
	}
	return n
}

func (x *wrappedLog) Logf(ctx context.Context, level slog.Level, format string, args ...any) {
	x.Logger.Log(ctx, level, fmt.Sprintf(format, args...))
}

func (x *wrappedLog) LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	filteredAttrs := make([]slog.Attr, 0, len(attrs))

	for _, attr := range attrs {
		if processed := filterStructFields(attr.Value.Any()); processed != nil {
			filteredAttrs = append(filteredAttrs, slog.Any(attr.Key, processed))
		} else {
			filteredAttrs = append(filteredAttrs, attr)
		}
	}

	x.Logger.LogAttrs(ctx, level, msg, filteredAttrs...)
}

func (x *wrappedLog) Debugf(format string, args ...any) {
	x.Logger.Debug(fmt.Sprintf(format, args...))
}

func (x *wrappedLog) Infof(format string, args ...any) {
	x.Logger.Info(fmt.Sprintf(format, args...))
}

func (x *wrappedLog) Warnf(format string, args ...any) {
	x.Logger.Warn(fmt.Sprintf(format, args...))
}

func (x *wrappedLog) Trace(msg string, args ...any) {
	x.Logger.Log(context.Background(), LevelTrace, msg, args...)
}

func (x *wrappedLog) Tracef(format string, args ...any) {
	x.Logger.Log(context.Background(), LevelTrace, fmt.Sprintf(format, args...))
}

func (x *wrappedLog) TraceContext(ctx context.Context, msg string, args ...any) {
	x.Logger.Log(ctx, LevelTrace, msg, args...)
}

func (x *wrappedLog) Errorf(msg string, args ...any) {
	x.Logger.Error(msg, args...)
}

type loggerOptions struct {
	sink  io.Writer
	hopts *slog.HandlerOptions
}

func defaultLoggerOptions() *loggerOptions {
	return &loggerOptions{
		sink: os.Stdout,
		hopts: &slog.HandlerOptions{
			AddSource:   false,
			Level:       slog.LevelInfo,
			ReplaceAttr: nil,
		},
	}
}

type funcLoggerOption struct {
	f func(*loggerOptions)
}

func (fdo *funcLoggerOption) apply(options *loggerOptions) {
	fdo.f(options)
}

func newFuncLoggerOption(f func(*loggerOptions)) *funcLoggerOption {
	return &funcLoggerOption{
		f: f,
	}
}

type LoggerOption interface {
	apply(*loggerOptions)
}

func WithSink(sink io.Writer) LoggerOption {
	return newFuncLoggerOption(func(o *loggerOptions) {
		o.sink = sink
	})
}

func WithLevel(level slog.Leveler) LoggerOption {
	return newFuncLoggerOption(func(o *loggerOptions) {
		o.hopts.Level = level
	})
}

func WithAddSource(addSource bool) LoggerOption {
	return newFuncLoggerOption(func(o *loggerOptions) {
		o.hopts.AddSource = addSource
	})
}

func Logger(opts ...LoggerOption) Interface {
	l := defaultLoggerOptions()
	for _, o := range opts {
		o.apply(l)
	}

	log := slog.New(slog.NewJSONHandler(l.sink, l.hopts))

	return &wrappedLog{
		Logger: log,
	}
}

// filterStructFields processes a struct using mapstructure while respecting `log` and `json` tags.
func filterStructFields(val any) any {
	t := reflect.TypeOf(val)
	if t == nil {
		return val
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// If it's not a struct, return as-is
	v := reflect.ValueOf(val)
	for v.IsValid() && v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct && !(v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.Struct) {
		return val
	}

	rawMap := make(map[string]any)
	if err := decode(val, &rawMap); err != nil {
		rawMap = map[string]any{
			"error": err.Error(),
		}
	}

	filtered := make(map[string]any)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		fieldValue := v.Field(i)

		// Get the JSON key name (default to field name if no tag)
		jsonKey := field.Name
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			jsonKey = strings.Split(jsonTag, ",")[0] // Handle `omitempty`
			if jsonKey == "-" {
				continue // Skip fields explicitly ignored in JSON
			}
			if jsonKey == "" {
				jsonKey = field.Name
			}
		}

		// Check for `log:"none"`
		logTag := field.Tag.Get("log")
		if logTag == "none" {
			continue // Exclude from logs
		}

		// Add the field to the filtered map if it exists in the rawMap
		if _, exists := rawMap[jsonKey]; exists {
			if logTag == "mask" {
				filtered[jsonKey] = "***"
			} else {
				filtered[jsonKey] = filterStructFields(fieldValue.Interface())
			}
		}
	}

	return filtered
}

func decode(input, output any, tagName ...string) error {
	if len(tagName) == 0 {
		tagName = []string{"json"}
	}
	config := &mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.RecursiveStructToMapHookFunc(),
			mapstructure.TextUnmarshallerHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
		),
		Metadata: nil,
		Result:   output,
		TagName:  tagName[0],
	}
	if decoder, err := mapstructure.NewDecoder(config); err != nil {
		return err
	} else {
		if err = decoder.Decode(input); err != nil {
			return err
		}
	}

	return nil
}

func Default() Interface {
	return &wrappedLog{
		Logger: slog.Default(),
	}
}

func (x *wrappedLog) Write(p []byte) (n int, err error) {
	ctx := context.Background()
	for i := LevelTrace; i <= slog.LevelError; i++ {
		if x.Logger.Enabled(ctx, i) {
			msg := string(p)
			msg = strings.TrimSpace(strings.ReplaceAll(msg, "\n", " "))
			x.Logger.Log(ctx, i, msg)
			return len(p), nil
		}
	}

	return 0, nil
}

func Writer(l Interface) io.Writer {
	return &wrappedLog{l.Wrapped()}
}

func SetDefault(opts ...LoggerOption) {
	if opts != nil {
		slog.SetDefault(Logger(opts...).Wrapped())
	} else {
		slog.SetDefault(Logger().Wrapped())
	}
}
