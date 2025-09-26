package br.com.microservices.orchestrated.productvalidationservice.core.consumers;

import br.com.microservices.orchestrated.productvalidationservice.core.services.ProductValidationService;
import br.com.microservices.orchestrated.productvalidationservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class ProductValidationConsumer {

    private final JsonUtil jsonUtil;

    private final ProductValidationService service;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.product-validation-success}"
    )
    public void consumeValidationSuccessEvent(String payload){

        log.info("Receiving event {} from product-validation-service success topic", payload);

        var event = jsonUtil.toEvent(payload);

        service.validateExistingProducts(event);
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.product-validation-fail}"
    )
    public void consumeValidationFailEvent(String payload){

        log.info("Receiving event {} from product-validation-service from fail topic", payload);

        var event = jsonUtil.toEvent(payload);

        service.rollbackEvent(event);
    }
}
