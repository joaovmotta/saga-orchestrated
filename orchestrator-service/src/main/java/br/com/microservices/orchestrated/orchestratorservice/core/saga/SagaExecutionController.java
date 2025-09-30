package br.com.microservices.orchestrated.orchestratorservice.core.saga;

import br.com.microservices.orchestrated.orchestratorservice.core.dtos.Event;
import br.com.microservices.orchestrated.orchestratorservice.core.enums.ETopics;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static br.com.microservices.orchestrated.orchestratorservice.core.saga.SagaHandler.EVENT_SOURCE_INDEX;
import static br.com.microservices.orchestrated.orchestratorservice.core.saga.SagaHandler.SAGA_HANDLER;
import static br.com.microservices.orchestrated.orchestratorservice.core.saga.SagaHandler.SAGA_STATUS_INDEX;
import static br.com.microservices.orchestrated.orchestratorservice.core.saga.SagaHandler.TOPIC_INDEX;
import static org.springframework.util.ObjectUtils.isEmpty;

@Component
@Slf4j
@AllArgsConstructor
public class SagaExecutionController {

    public ETopics getNextTopic(Event event){

        if(isEmpty(event.getStatus()) || isEmpty(event.getSource())){

            throw new ValidateException("Source and status must be informed");
        }

        ETopics topic = findTopic(event);

        logCurrentSaga(event, topic);

        return topic;
    }

    private ETopics findTopic(Event event){

        return (ETopics) Arrays.stream(SAGA_HANDLER)
                .filter(row -> isEventValid(event, row))
                .map(i -> i[TOPIC_INDEX])
                .findFirst()
                .orElseThrow(() -> {throw new ValidateException("Topic not found");});
    }

    private Boolean isEventValid(Event event, Object[] row){

        var source = row[EVENT_SOURCE_INDEX];
        var status = row[SAGA_STATUS_INDEX];

        return event.getSource().equals(source)
                && event.getStatus().equals(status);
    }

    private void logCurrentSaga(Event event, ETopics eTopic){

        var sagaId = createSagaId(event);

        var source = event.getSource();

        switch (event.getStatus()){

            case SUCCESS -> log.info("### CURRENT SAGA: {} | SUCCESS | NEXT TOPIC {} | {}", source, eTopic, sagaId);
            case ROLLBACK_PENDING -> log.info("### CURRENT SAGA: {} | SENDING ROLLBACK PENDING SERVICE | NEXT TOPIC {} | {}", source, eTopic, sagaId);
            case FAILED -> log.info("### CURRENT SAGA: {} | SENDING TO ROLLBACK PREVIOUS SERVICE | NEXT TOPIC {} | {}", source, eTopic, sagaId);
        }
    }

    private String createSagaId(Event event){

        return String.format("ORDER ID: %s | TRANSACTION ID: %s | EVENT ID: %s",
                event.getOrderId(), event.getTransactionId(), event.getId());
    }
}
