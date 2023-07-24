package com.example.opentelemetry.otlp.server;

import io.grpc.HandlerRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerMethodDefinition;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class OtlpServer {
    final Logger logger = LoggerFactory.getLogger(getClass());

    private Server server;

    public static void main(String[] args) throws InterruptedException, IOException {

        AutoConfiguredOpenTelemetrySdk
                .builder()
                .addResourceCustomizer((resource, configProperties) -> Resource.builder().put(ResourceAttributes.SERVICE_NAME, "service-graph-generator").build())
                .setResultAsGlobal()
                .build();

        OtlpServer otlpServer = new OtlpServer();
        otlpServer.start();
        otlpServer.blockUntilShutdown();

    }

    public void start() throws IOException {
        Meter meter = GlobalOpenTelemetry.getMeter("service-graph-generator");

        this.server = ServerBuilder
                .forPort(4316)
                .maxInboundMessageSize(100 * 1024)
//                .executor(Executors.newFixedThreadPool(50))
                .addService(new TracesHandler(meter))
                .addService(new LogsHandler(meter))
                .addService(new MetricsHandler(meter))
                .fallbackHandlerRegistry(new HandlerRegistry() {
                    @Nullable
                    @Override
                    public ServerMethodDefinition<?, ?> lookupMethod(String methodName, @Nullable String authority) {
                        return null;
                    }
                })
                .build();
        server.start();

        logger.info("OTLP Server started on port: " + server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            logger.debug("*** shutting down gRPC server since JVM is shutting down");
            try {
                stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            logger.info("*** server shut down");
        }));

    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread as the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
