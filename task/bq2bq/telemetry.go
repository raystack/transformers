package main

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

func InitTelemetry(l hclog.Logger, tracingAddr string) (func(), error) {
	var tp *tracesdk.TracerProvider
	var err error
	if tracingAddr != "" {
		l.Info("enabling jaeger traces", "addr", tracingAddr)
		tp, err = tracerProvider(tracingAddr)
		if err != nil {
			return nil, err
		}

		// Register our TracerProvider as the global so any imported
		// instrumentation in the future will default to using it.
		otel.SetTracerProvider(tp)

		// Required to receive trace info from upstream
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	}

	return func() {
		if tp != nil {
			if err = tp.Shutdown(context.Background()); err != nil {
				l.Warn("failed to shutdown trace provider", "err", err)
			}
		}
	}, nil
}

// tracerProvider returns an OpenTelemetry TracerProvider configured to use
// the Jaeger exporter that will send spans to the provided url. The returned
// TracerProvider will also use a Resource configured with all the information
// about the application.
func tracerProvider(url string) (*tracesdk.TracerProvider, error) {
	// create the Jaeger exporter
	jaegerExporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production
		tracesdk.WithBatcher(jaegerExporter),

		// Record information about this application in an Resource
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(Name),
			semconv.ServiceVersionKey.String(Version),
		)),
	)

	return tp, nil
}

func StartChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("bq2bq")

	return tracer.Start(ctx, name)
}
