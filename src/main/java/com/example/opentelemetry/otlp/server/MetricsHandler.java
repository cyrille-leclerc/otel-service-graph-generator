package com.example.opentelemetry.otlp.server;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

public class MetricsHandler extends MetricsServiceGrpc.MetricsServiceImplBase {
    final Logger logger = LoggerFactory.getLogger(getClass());
    final LongCounter processedMetricCounter;

    public MetricsHandler(Meter meter) {
        processedMetricCounter = meter.counterBuilder("processed_log_records").setDescription("Number of log records processed by the service graph processor").build();
    }

    @Override
    public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                processedMetricCounter.add(scopeMetrics.getMetricsCount());
            }
            responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}
