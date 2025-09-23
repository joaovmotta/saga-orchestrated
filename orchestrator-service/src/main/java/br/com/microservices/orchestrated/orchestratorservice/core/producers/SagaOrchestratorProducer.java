package br.com.microservices.orchestrated.orchestratorservice.core.producers;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class SagaOrchestratorProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendEvent(String topic, String payload) {

        try {

            log.info("Sending event to topic {} with data {}", topic, payload);

            kafkaTemplate.send(topic, payload);
        } catch (Exception e) {

            log.error("Error trying to send data to topic {} with data {}", topic, payload, e);
        }
    }
}
