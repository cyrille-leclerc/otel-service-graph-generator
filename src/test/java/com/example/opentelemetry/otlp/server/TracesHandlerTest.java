package com.example.opentelemetry.otlp.server;

import com.google.protobuf.ByteString;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

class TracesHandlerTest {

    @org.junit.jupiter.api.Test
    void export() {


        Span TEST_SPAN_1 = Span.newBuilder()
                .setTraceId(ByteString.copyFromUtf8("TEST_TRACE_ID_1"))
                .setSpanId(ByteString.copyFromUtf8("TEST_SPAN_ID_1"))
                .setName("TEST_NAME_1")
                .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER)
                .setStartTimeUnixNano(100)
                .setEndTimeUnixNano(101)
                .setTraceState("SUCCESS").build();

        Span TEST_SPAN_2 = Span.newBuilder()
                .setTraceId(ByteString.copyFromUtf8("TEST_TRACE_ID_2"))
                .setSpanId(ByteString.copyFromUtf8("TEST_SPAN_ID_2"))
                .setName("TEST_NAME_2")
                .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER)
                .setStartTimeUnixNano(100)
                .setEndTimeUnixNano(101)
                .setTraceState("SUCCESS").build();

        ScopeSpans scopeSpans = ScopeSpans.newBuilder()
                .addSpans(TEST_SPAN_1)
                .addSpans(TEST_SPAN_2)
                .setScope(InstrumentationScope.newBuilder().setName("my.library").setVersion("1.0.0"))
                .build();
        ResourceSpans resourceSpans = ResourceSpans.newBuilder()
                .addScopeSpans(scopeSpans)
                .setResource(Resource.newBuilder()
                        .addAttributes(KeyValue.newBuilder()
                                .setKey("service.name")
                                .setValue(AnyValue.newBuilder().setStringValue("my.service"))
                        ))
                .build();

        ExportTraceServiceRequest SUCCESS_REQUEST = ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(resourceSpans)
                .build();

        TracesHandler tracesHandler = new TracesHandler(GlobalOpenTelemetry.getMeter("test"));
        tracesHandler.export(SUCCESS_REQUEST, new ClientResponseObserver<ExportTraceServiceRequest, ExportTraceServiceResponse>() {
            @Override
            public void onNext(ExportTraceServiceResponse value) {
                System.out.println("onNext: " + value);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("onError");
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                System.out.println("onCompleted");
            }

            @Override
            public void beforeStart(ClientCallStreamObserver<ExportTraceServiceRequest> requestStream) {
                System.out.println("beforeStart");
            }
        });

    }
}