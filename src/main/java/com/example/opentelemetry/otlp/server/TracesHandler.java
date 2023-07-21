package com.example.opentelemetry.otlp.server;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TracesHandler extends TraceServiceGrpc.TraceServiceImplBase {
    final Logger logger = LoggerFactory.getLogger(getClass());

    final DoubleHistogram httpClientDuration;
    final DoubleHistogram rpcClientDuration;
    final DoubleHistogram dbClientDuration;
    final DoubleHistogram otherClientDuration;
    final DoubleHistogram messagingProducerDuration;
    final DoubleHistogram otherProducerDuration;
    final DoubleHistogram httpServerDuration;
    final DoubleHistogram rpcServerDuration;
    final DoubleHistogram otherServerDuration;
    final DoubleHistogram messagingConsumerDuration;
    final DoubleHistogram otherConsumerDuration;

    final LongCounter processedSpansCounter;

    public TracesHandler(Meter meter) {
        httpClientDuration = meter.histogramBuilder("http.client.duration").setUnit("s").build();
        rpcClientDuration = meter.histogramBuilder("rpc.client.duration").setUnit("s").build();
        dbClientDuration = meter.histogramBuilder("db.client.duration").setUnit("s").build();
        otherClientDuration = meter.histogramBuilder("other.client.duration").setUnit("s").build();
        messagingProducerDuration = meter.histogramBuilder("messaging.producer.duration").setUnit("s").build();
        otherProducerDuration = meter.histogramBuilder("other.producer.duration").setUnit("s").build();

        httpServerDuration = meter.histogramBuilder("http.server.duration").setUnit("s").build();
        rpcServerDuration = meter.histogramBuilder("rpc.server.duration").setUnit("s").build();
        otherServerDuration = meter.histogramBuilder("other.server.duration").setUnit("s").build();
        messagingConsumerDuration = meter.histogramBuilder("messaging.consumer.duration").setUnit("s").build();
        otherConsumerDuration = meter.histogramBuilder("other.consumer.duration").setUnit("s").build();

        processedSpansCounter = meter.counterBuilder("processed_spans").setDescription("Number of spans processed by the service graph processor").build();
    }

    enum TelemetrySdk {OPEN_TELEMETRY, JAEGER, OPEN_CENSUS, ZIPKIN, OTHER}

    @Override
    public void export(ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> responseObserver) {
        long nanosBefore = System.nanoTime();
        for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                for (Span span : scopeSpans.getSpansList()) {
                    processedSpansCounter.add(1);

                    Span.SpanKind spanKind = span.getKind();
                    if (spanKind == Span.SpanKind.SPAN_KIND_INTERNAL || spanKind == Span.SpanKind.SPAN_KIND_UNSPECIFIED) {
                        logger.trace("Skip span.name=\"{}\" of span.kind={}", span.getName(), span.getKind());
                        return;
                    }

                    AttributesBuilder metricAttributes = Attributes.builder();

                    Map<String, AnyValue> resourceAttributes = resourceSpans.getResource().getAttributesList().stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

                    String serviceName = resourceAttributes.get(ResourceAttributes.SERVICE_NAME.getKey()).getStringValue();

                    TelemetrySdk telemetrySdk = Optional.ofNullable(resourceAttributes.get(ResourceAttributes.TELEMETRY_SDK_NAME.getKey()))
                            .map(sdkName -> "opentelemetry".equals(sdkName.getStringValue()) ? TelemetrySdk.OPEN_TELEMETRY : TelemetrySdk.OTHER)
                            .orElseGet(() ->
                                    Optional.ofNullable(resourceAttributes.get("opencensus.exporterversion"))
                                            .filter(exporter -> exporter.getStringValue().startsWith("Jaeger"))
                                            .map(exporter -> TelemetrySdk.JAEGER)
                                            .orElse(TelemetrySdk.OTHER));

                    // TODO add resource attributes as metric resource attributes, not as metric attributes.
                    copyStringResourceAttributeIfExist(resourceAttributes, metricAttributes, ResourceAttributes.SERVICE_NAME);
                    copyStringResourceAttributeIfExist(resourceAttributes, metricAttributes, ResourceAttributes.SERVICE_NAMESPACE);
                    copyStringResourceAttributeIfExist(resourceAttributes, metricAttributes, ResourceAttributes.SERVICE_VERSION);
                    copyStringResourceAttributeIfExist(resourceAttributes, metricAttributes, ResourceAttributes.SERVICE_INSTANCE_ID);

                    copyStringResourceAttributeIfExist(resourceAttributes, metricAttributes, ResourceAttributes.DEPLOYMENT_ENVIRONMENT);

                    Map<String, AnyValue> spanAttributes = span.getAttributesList().stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

                    metricAttributes.put("status.code", span.hasStatus() ? span.getStatus().getCodeValue() : -1); // TODO

                    double durationInSeconds = (double) (span.getEndTimeUnixNano() - span.getStartTimeUnixNano()) / 1_000_000_000;

                    metricAttributes.put("original_instrumentation_scope", telemetrySdk == TelemetrySdk.OPEN_TELEMETRY ? scopeSpans.getScope().getName() : "#" + telemetrySdk + "#"); // TODO

                    switch (spanKind) {
                        case SPAN_KIND_SERVER -> {
                            switch (telemetrySdk) {
                                case OPEN_TELEMETRY -> {
                                    metricAttributes.put("operation", span.getName());
                                    copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.NET_HOST_NAME); // TODO HANDLE FALLBACKS
                                    if (spanAttributes.containsKey(SemanticAttributes.HTTP_METHOD.getKey())) {
                                        // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/http/http-spans.md#http-server
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.HTTP_METHOD);
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.HTTP_ROUTE);
                                        copyLongAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.HTTP_STATUS_CODE);

                                        logger.trace("Record duration for HTTP server service={}, span=\"{}\"", serviceName, span.getName());

                                        httpServerDuration.record(durationInSeconds, metricAttributes.build());
                                    } else if (spanAttributes.containsKey(SemanticAttributes.RPC_SYSTEM.getKey())) {
                                        // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/rpc/rpc-spans.md
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.RPC_SYSTEM);
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.RPC_METHOD);
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.RPC_SERVICE);
                                        copyLongAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.RPC_GRPC_STATUS_CODE);

                                        logger.trace("Record duration for RPC server service={}, span=\"{}\"", serviceName, span.getName());

                                        rpcServerDuration.record(durationInSeconds, metricAttributes.build());
                                    } else {
                                        metricAttributes.put("original_instrumentation_scope", "TODO"); // TODO

                                        logger.debug("Record duration for other server service={}, span=\"{}\"", serviceName, span.getName());

                                        otherServerDuration.record(durationInSeconds, metricAttributes.build());
                                    }
                                }
                                case JAEGER -> {
                                    if (spanAttributes.containsKey("http.method")) {
                                        metricAttributes.put("operation", span.getName());

                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, "http.method", SemanticAttributes.HTTP_METHOD);
                                        copyLongAttributeIfExist(spanAttributes, metricAttributes, "http.status_code", SemanticAttributes.HTTP_STATUS_CODE);
                                        // TODO extract NET_PEER_NAME from "http.url" (eg "http://localhost:8088/shipOrder")

                                        logger.trace("Record duration for Jaeger HTTP server service={}, span=\"{}\"", serviceName, span.getName());

                                        httpServerDuration.record(durationInSeconds, metricAttributes.build());
                                    }
                                }
                            }
                        }
                        case SPAN_KIND_CONSUMER -> {
                            metricAttributes.put("operation", span.getName());
                            if (spanAttributes.containsKey(SemanticAttributes.MESSAGING_SYSTEM.getKey())) {
                                copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.MESSAGING_SYSTEM);

                                logger.trace("Record duration for messaging consumer service={}, span=\"{}\"", serviceName, span.getName());

                                messagingConsumerDuration.record(durationInSeconds, metricAttributes.build());
                            } else {
                                metricAttributes.put("original_instrumentation_scope", "TODO"); // TODO

                                logger.debug("Record duration for other consumer service={}, span=\"{}\"", serviceName, span.getName());

                                otherConsumerDuration.record(durationInSeconds, metricAttributes.build());
                            }
                        }
                        case SPAN_KIND_CLIENT -> {
                            switch (telemetrySdk) {
                                case OPEN_TELEMETRY -> {
                                    metricAttributes.put("operation", "TODO"); // TODO
                                    copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.NET_PEER_NAME); // TODO HANDLE FALLBACKS
                                    if (spanAttributes.containsKey(SemanticAttributes.HTTP_METHOD.getKey())) {
                                        // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/http/http-spans.md#http-client
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.HTTP_METHOD);
                                        copyLongAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.HTTP_STATUS_CODE);

                                        logger.trace("Record duration for http client service={}, span=\"{}\"", serviceName, span.getName());

                                        httpClientDuration.record(durationInSeconds, metricAttributes.build());
                                    } else if (spanAttributes.containsKey(SemanticAttributes.RPC_SYSTEM.getKey())) {
                                        // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/rpc/rpc-spans.md#client-attributes
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.RPC_SYSTEM);
                                        copyLongAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.RPC_GRPC_STATUS_CODE);

                                        logger.debug("Record duration for RPC client service={}, span=\"{}\"", serviceName, span.getName());

                                        rpcClientDuration.record(durationInSeconds, metricAttributes.build());
                                    } else if (spanAttributes.containsKey(SemanticAttributes.DB_SYSTEM.getKey())) {
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.DB_SYSTEM);
                                        copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.DB_NAME);
                                        logger.trace("Record duration for DB client service={}, span=\"{}\"", serviceName, span.getName());

                                        dbClientDuration.record(durationInSeconds, metricAttributes.build());
                                    } else if (spanAttributes.containsKey(SemanticAttributes.MESSAGING_SYSTEM.getKey())) {
                                        logger.trace("Skip messaging client span (usually an 'ack' message service={}, span=\"{}\"", serviceName, span.getName());
                                    } else {

                                        logger.debug("Record duration for other client service={}, span=\"{}\"", serviceName, span.getName());

                                        otherClientDuration.record(durationInSeconds, metricAttributes.build());
                                    }
                                }
                                case JAEGER -> {
                                    if (spanAttributes.containsKey("db.type")) {
                                        if (spanAttributes.containsKey("db.statement")) {
                                            copyStringAttributeIfExist(spanAttributes, metricAttributes, "db.instance", SemanticAttributes.DB_NAME);
                                            copyStringAttributeIfExist(spanAttributes, metricAttributes, "db.type", SemanticAttributes.DB_SYSTEM);
                                            // TODO Jaeger "peer.address" is "host:port"
                                            copyStringAttributeIfExist(spanAttributes, metricAttributes, "peer.address", SemanticAttributes.NET_HOST_NAME);

                                            logger.trace("Record duration for Jaeger db client service={}, span=\"{}\"", serviceName, span.getName());

                                            dbClientDuration.record(durationInSeconds, metricAttributes.build());
                                        } else {
                                            logger.trace("Skip unneeded jdbc span (usually a 'close' span): {}", span.getName());
                                        }
                                    }
                                }
                            }

                        }
                        case SPAN_KIND_PRODUCER -> {
                            metricAttributes.put("operation", span.getName());
                            if (spanAttributes.containsKey(SemanticAttributes.MESSAGING_SYSTEM.getKey())) {
                                copyStringAttributeIfExist(spanAttributes, metricAttributes, SemanticAttributes.MESSAGING_SYSTEM);

                                logger.debug("Record duration for messaging producer service={}, span=\"{}\"", serviceName, span.getName());

                                messagingProducerDuration.record(durationInSeconds, metricAttributes.build());
                            } else {
                                logger.debug("Record duration for other producer service={}, span=\"{}\"", serviceName, span.getName());

                                otherProducerDuration.record(durationInSeconds, metricAttributes.build());
                            }
                        }
                        default -> logger.info("Unexpected span kind " + span.getKind() + " - " + span.getName());
                    }
                }
            }
        }
        long nanosBeforeOnNext = System.nanoTime();

        responseObserver.onNext(ExportTraceServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
        logger.debug("export.duration=" + TimeUnit.MILLISECONDS.convert(System.nanoTime() - nanosBefore, TimeUnit.NANOSECONDS) + "ms, " +
                "processing.duration=" + TimeUnit.MILLISECONDS.convert(nanosBeforeOnNext - nanosBefore, TimeUnit.NANOSECONDS) + "ms, " +
                "request.size=" + request.getSerializedSize() + "bytes");
    }

    private void copyStringResourceAttributeIfExist(Map<String, AnyValue> resourceAttributes, AttributesBuilder attributesBuilder, AttributeKey<String> attributeKey) {
        Optional.ofNullable(resourceAttributes.get(attributeKey.getKey())).ifPresent(value -> attributesBuilder.put(attributeKey, value.getStringValue()));
    }

    private static void copyStringAttributeIfExist(Map<String, AnyValue> attributes, AttributesBuilder attributesBuilder, AttributeKey<String> attributeKey) {
        Optional.ofNullable(attributes.get(attributeKey.getKey())).ifPresent(value -> attributesBuilder.put(attributeKey, value.getStringValue()));
    }

    private static void copyStringAttributeIfExist(Map<String, AnyValue> spanAttributes, AttributesBuilder metricAttributes, String spanAttributeKey, AttributeKey<String> metricAttributeKey) {
        Optional.ofNullable(spanAttributes.get(spanAttributeKey)).ifPresent(value -> metricAttributes.put(metricAttributeKey, value.getStringValue()));
    }

    private static void copyLongAttributeIfExist(Map<String, AnyValue> spanAttributes, AttributesBuilder metricAttributes, AttributeKey<Long> attributeKey) {
        Optional.ofNullable(spanAttributes.get(attributeKey.getKey())).ifPresent(value -> metricAttributes.put(attributeKey, value.getIntValue()));
    }

    private static void copyLongAttributeIfExist(Map<String, AnyValue> spanAttributes, AttributesBuilder metricAttributes, String spanAttributeKey, AttributeKey<Long> metricAttributeKey) {
        Optional.ofNullable(spanAttributes.get(spanAttributeKey)).ifPresent(value -> metricAttributes.put(metricAttributeKey, value.getIntValue()));
    }
}
