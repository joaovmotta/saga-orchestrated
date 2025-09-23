package br.com.microservices.orchestrated.paymentservice.core.consumers;

import br.com.microservices.orchestrated.paymentservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class ProductValidationConsumer {

    private final JsonUtil jsonUtil;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-success}"
    )
    public void consumePaymentSuccessEvent(String payload){

        log.info("Receiving event {} from payment-service payment-success topic", payload);

        var event = jsonUtil.toEvent(payload);

        log.info(event.toString());
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-fail}"
    )
    public void consumePaymentFailEvent(String payload){

        log.info("Receiving event {} from payment-service payment-fail topic", payload);

        var event = jsonUtil.toEvent(payload);

        log.info(event.toString());
    }
}
