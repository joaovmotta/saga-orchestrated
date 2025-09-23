package br.com.microservices.orchestrated.orchestratorservice.core.consumers;

import br.com.microservices.orchestrated.orchestratorservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class SagaOrchestratorConsumer {

    private final JsonUtil jsonUtil;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.start-saga}"
    )
    public void consumeStartSagaTopic(String payload){

        log.info("Receiving event {} from start-saga topic", payload);

        var event = jsonUtil.toEvent(payload);

        log.info(event.toString());
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.orchestrator}"
    )
    public void consumeOrchestratorTopic(String payload){

        log.info("Receiving event {} from orchestrator topic", payload);

        var event = jsonUtil.toEvent(payload);

        log.info(event.toString());
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.finish-success}"
    )
    public void consumeFinishSuccessTopic(String payload){

        log.info("Receiving event {} from finish-success topic", payload);

        var event = jsonUtil.toEvent(payload);

        log.info(event.toString());
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.finish-fail}"
    )
    public void consumeFinishFailTopic(String payload){

        log.info("Receiving event {} from finish-fail topic", payload);

        var event = jsonUtil.toEvent(payload);

        log.info(event.toString());
    }
}
