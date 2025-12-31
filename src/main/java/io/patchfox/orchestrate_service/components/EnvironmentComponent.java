package io.patchfox.orchestrate_service.components;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Getter;


@Getter
@Component
public class EnvironmentComponent {
    
    @Value("${spring.application.name}")
    String serviceName;

    @Value("${spring.kafka.request-topic}")
    String kafkaRequestTopicName;

    @Value("${spring.kafka.response-topic}")
    String kafkaResponseTopicName;

    @Value("${spring.kafka.request.client-id-prefix}")
    String kafkaRequestClientIdPrefix;

    @Value("${spring.kafka.response.client-id-prefix}")
    String kafkaResponseClientIdPrefix;

    @Value("${spring.kafka.group-name}")
    String kafkaGroupName;

    @Value("${io.patchfox.turbo.orchestrate-service.oss-enrichment.buffer.size}")
    Integer ossEnrichmentKafkaBufferSize;
    
    @Value("${io.patchfox.turbo.orchestrate-service.oss-enrichment.nap.duration-ms}")
    Long ossEnrichmentKafkaNapDurationMs;

    @Value("${io.patchfox.turbo.orchestrate-service.analyze.page-size}")
    Integer analyzePageSize;

    @Value("${io.patchfox.turbo.orchestrate-service.peristalsis.enabled}")
    boolean peristalsisEnabled;
}
