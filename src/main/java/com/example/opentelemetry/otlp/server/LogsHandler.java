package com.example.opentelemetry.otlp.server;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogsHandler extends LogsServiceGrpc.LogsServiceImplBase {
    final Logger logger = LoggerFactory.getLogger(getClass());

    final LongCounter processedLogRecordCounter;
    public LogsHandler(Meter meter) {
        processedLogRecordCounter = meter.counterBuilder("processed_log_records").setDescription("Number of log records processed by the service graph processor").build();
    }

    @Override
    public void export(ExportLogsServiceRequest request, StreamObserver<ExportLogsServiceResponse> responseObserver) {
        for (ResourceLogs resourceLogs : request.getResourceLogsList()) {
            for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                processedLogRecordCounter.add(scopeLogs.getLogRecordsCount());
            }
        }
        responseObserver.onNext(ExportLogsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
