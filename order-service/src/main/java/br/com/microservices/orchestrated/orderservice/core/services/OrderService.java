package br.com.microservices.orchestrated.orderservice.core.services;

import br.com.microservices.orchestrated.orderservice.core.document.Event;
import br.com.microservices.orchestrated.orderservice.core.document.Order;
import br.com.microservices.orchestrated.orderservice.core.dtos.OrderRequest;
import br.com.microservices.orchestrated.orderservice.core.producers.SagaProducer;
import br.com.microservices.orchestrated.orderservice.core.repository.OrderRepository;
import br.com.microservices.orchestrated.orderservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@AllArgsConstructor
public class OrderService {

    private static final String TRANSACTION_ID_PATTERN = "%s_%s";

    private final OrderRepository orderRepository;

    private final JsonUtil jsonUtil;

    private final SagaProducer sagaProducer;

    private final EventService eventService;

    public Order createOrder(OrderRequest orderRequest){

        String transactionId = String.format(TRANSACTION_ID_PATTERN, Instant.now().toEpochMilli(), UUID.randomUUID());

        Order order = Order.builder()
                .products(orderRequest.getProducts())
                .createdAt(LocalDateTime.now())
                .transactionId(transactionId)
                .build();

        orderRepository.save(order);

        sagaProducer.sendEvent(jsonUtil.toJson(createPayload(order)));

        return order;
    }

    private Event createPayload(Order order){

        var event = Event.builder()
                .orderId(order.getId())
                .transactionId(order.getTransactionId())
                .payload(order)
                .createdAt(LocalDateTime.now())
                .build();

        return eventService.save(event);
    }
}
