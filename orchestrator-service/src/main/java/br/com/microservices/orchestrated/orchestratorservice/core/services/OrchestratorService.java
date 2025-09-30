package br.com.microservices.orchestrated.orchestratorservice.core.services;

import br.com.microservices.orchestrated.orchestratorservice.core.dtos.Event;
import br.com.microservices.orchestrated.orchestratorservice.core.dtos.History;
import br.com.microservices.orchestrated.orchestratorservice.core.enums.ETopics;
import br.com.microservices.orchestrated.orchestratorservice.core.producers.SagaOrchestratorProducer;
import br.com.microservices.orchestrated.orchestratorservice.core.saga.SagaExecutionController;
import br.com.microservices.orchestrated.orchestratorservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.orchestratorservice.core.enums.EEventSource.ORCHESTRATOR;
import static br.com.microservices.orchestrated.orchestratorservice.core.enums.ESagaStatus.FAILED;
import static br.com.microservices.orchestrated.orchestratorservice.core.enums.ESagaStatus.SUCCESS;

@Service
@Slf4j
@AllArgsConstructor
public class OrchestratorService {

    private final SagaOrchestratorProducer sagaOrchestratorProducer;

    private final JsonUtil jsonUtil;

    private final SagaExecutionController sagaExecutionController;

    public void startSaga(Event event){

        event.setSource(ORCHESTRATOR);
        event.setStatus(SUCCESS);

        var topic = this.getTopic(event);

        log.info("SAGA STARTED");
        addHistory(event, "Saga started");

        sagaOrchestratorProducer.sendEvent(topic.getTopic(), jsonUtil.toJson(event));
    }

    public void finishSagaSuccess(Event event){

        event.setSource(ORCHESTRATOR);
        event.setStatus(SUCCESS);

        log.info("SAGA FINISHED SUCCESSFULLY FOR EVENT {}", event.getId());
        addHistory(event, "Saga finished");

        notifyFinishedSaga(event);
    }

    private void notifyFinishedSaga(Event event){

        sagaOrchestratorProducer.sendEvent(ETopics.NOTIFY_ENDING.getTopic(), jsonUtil.toJson(event));
    }

    public void finishSagaFailed(Event event){

        event.setSource(ORCHESTRATOR);
        event.setStatus(FAILED);

        log.info("SAGA FINISHED WITH ERRORS FOR EVENT {}", event.getId());
        addHistory(event, "Saga finished with errors");

        notifyFinishedSaga(event);
    }

    public void continueSaga(Event event){

        ETopics topic = this.getTopic(event);

        log.info("SAGA CONTINUE FOR EVENT {}", event.getId());

        sagaOrchestratorProducer.sendEvent(topic.getTopic(), jsonUtil.toJson(event));
    }

    private ETopics getTopic(Event event){

        return sagaExecutionController.getNextTopic(event);
    }
    private void addHistory(Event event, String message) {

        History history = History.builder()
                .source(event.getSource())
                .status(event.getStatus())
                .message(message)
                .createdAt(LocalDateTime.now())
                .build();

        event.addToHistory(history);
    }
}
