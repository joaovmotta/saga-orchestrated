package br.com.microservices.orchestrated.paymentservice.core.services;

import br.com.microservices.orchestrated.paymentservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.paymentservice.core.dtos.Event;
import br.com.microservices.orchestrated.paymentservice.core.dtos.History;
import br.com.microservices.orchestrated.paymentservice.core.dtos.OrderProducts;
import br.com.microservices.orchestrated.paymentservice.core.enums.EPaymentStatus;
import br.com.microservices.orchestrated.paymentservice.core.enums.ESagaStatus;
import br.com.microservices.orchestrated.paymentservice.core.models.Payment;
import br.com.microservices.orchestrated.paymentservice.core.producers.KafkaProducer;
import br.com.microservices.orchestrated.paymentservice.core.repository.PaymentRepository;
import br.com.microservices.orchestrated.paymentservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.paymentservice.core.enums.EPaymentStatus.REFUND;
import static br.com.microservices.orchestrated.paymentservice.core.enums.EPaymentStatus.SUCCESS;
import static br.com.microservices.orchestrated.paymentservice.core.enums.ESagaStatus.FAILED;
import static br.com.microservices.orchestrated.paymentservice.core.enums.ESagaStatus.ROLLBACK_PENDING;

@Service
@Slf4j
@AllArgsConstructor
public class PaymentService {

    private static final String CURRENT_SOURCE = "PAYMENT_SERVICE";

    private static final Double REDUCE_SUM_VALUE = 0.0;

    private final JsonUtil jsonUtil;

    private final KafkaProducer producer;

    private final PaymentRepository repository;

    public void realizePayment(Event event){

        try {

            checkCurrentValidation(event);

            createPendingPayment(event);

            Payment payment = this.findByOrderIdAndTransactionId(event);

            validateAmount(payment.getTotalAmount());

            changePaymentToSuccess(payment);

            handleSuccess(event);

        }catch (Exception e){

            handleFailCurrentNotExecuted(event, e.getMessage());

            log.error("Error trying to make payment: ", e);
        }

        producer.sendEvent(jsonUtil.toJson(event));
    }

    public void realizeRefund(Event event){

        event.setStatus(FAILED);
        event.setSource(CURRENT_SOURCE);

        try{
            changePaymentToRefund(event);
            addHistory(event, "Rollback executed for payment");
        }catch (Exception e){
            addHistory(event, "Rollback not executed for payment");
        }

        producer.sendEvent(jsonUtil.toJson(event));
    }

    private void changePaymentToRefund(Event event){

        Payment payment = findByOrderIdAndTransactionId(event);

        payment.setStatus(REFUND);

        setEventAmountItems(event, payment);

        save(payment);
    }

    private void handleFailCurrentNotExecuted(Event event, String message) {

        event.setStatus(ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);

        addHistory(event, "Fail to realize payment: " + message);
    }

    private void handleSuccess(Event event) {

        event.setStatus(ESagaStatus.SUCCESS);
        event.setSource(CURRENT_SOURCE);

        addHistory(event, "Payment realized with success");

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

    private void changePaymentToSuccess(Payment payment){

        payment.setStatus(SUCCESS);

        repository.save(payment);
    }

    private void validateAmount(Double amount){

        if(amount < 0.1){
            throw new ValidationException("The minimum value for payment is 0.1");
        }
    }

    private void createPendingPayment(Event event) {

        Payment payment = Payment.builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .totalAmount(calculateAmount(event))
                .totalItems(calculateTotalItems(event))
                .build();

        this.save(payment);

        setEventAmountItems(event, payment);
    }

    private void setEventAmountItems(Event event, Payment payment){

        event.getPayload().setTotalAmount(payment.getTotalAmount());
        event.getPayload().setTotalItems(payment.getTotalItems());
    }

    private double calculateAmount(Event event){

        return event
                .getPayload()
                .getProducts()
                .stream()
                .map(product -> product.getQuantity() * product.getProduct().getUnitValue())
                .reduce(REDUCE_SUM_VALUE, Double::sum);
    }

    private int calculateTotalItems(Event event){

        return event
                .getPayload()
                .getProducts()
                .stream()
                .map(OrderProducts::getQuantity)
                .reduce(REDUCE_SUM_VALUE.intValue(), Integer::sum);
    }

    private Payment findByOrderIdAndTransactionId(Event event){

        return this.repository.findByOrderIdAndTransactionId(event.getOrderId(), event.getTransactionId())
                .orElseThrow(() -> new ValidationException("Payment not found by order and transaction"));
    }

    private void save(Payment payment){

        repository.save(payment);
    }

    private void checkCurrentValidation(Event event) {

        if (repository.existsByOrderIdAndTransactionId(event.getOrderId(), event.getTransactionId())) {

            throw new ValidationException("Theres another transaction for this payment");
        }

    }
}
