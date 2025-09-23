package br.com.microservices.orchestrated.orderservice.core.services;

import br.com.microservices.orchestrated.orderservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.orderservice.core.document.Event;
import br.com.microservices.orchestrated.orderservice.core.dtos.EventFilters;
import br.com.microservices.orchestrated.orderservice.core.repository.EventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

import static org.springframework.util.ObjectUtils.isEmpty;

@Slf4j
@Service
@AllArgsConstructor
public class EventService {

    private final EventRepository eventRepository;

    public Event save(Event event){

        return eventRepository.save(event);
    }

    public List<Event> findAll(){

        return eventRepository.findAllByOrderByCreatedAtDesc();
    }

    public Event findByFilters(EventFilters filters){

        validateEmptyFilters(filters);

        if(!isEmpty(filters.getOrderId())){

            return this.findByOrderId(filters.getOrderId());
        }else {

            return this.findByTransactionId(filters.getTransactionId());
        }
    }

    private Event findByOrderId(String orderId){

        return eventRepository
                .findTop1ByOrderIdOrderByCreatedAtDesc(orderId)
                .orElseThrow(() -> new ValidationException("Event not found by order id"));
    }

    private Event findByTransactionId(String orderId){

        return eventRepository
                .findTop1ByTransactionIdOrderByCreatedAtDesc(orderId)
                .orElseThrow(() -> new ValidationException("Event not found by transaction id"));
    }

    private void validateEmptyFilters(EventFilters filters){

        if (isEmpty(filters.getOrderId()) && isEmpty(filters.getTransactionId())){

            throw new ValidationException("OrderID or TransactionID must be informed.");
        }
    }

    public void notifyEnding(Event event){

        event.setOrderId(event.getOrderId());
        event.setCreatedAt(LocalDateTime.now());

        this.save(event);

        log.info("Order {} with saga notified. TransactionId: {}", event.getOrderId(), event.getTransactionId());
    }
}
