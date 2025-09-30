package br.com.microservices.orchestrated.productvalidationservice.core.services;

import br.com.microservices.orchestrated.productvalidationservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.productvalidationservice.core.dtos.Event;
import br.com.microservices.orchestrated.productvalidationservice.core.dtos.History;
import br.com.microservices.orchestrated.productvalidationservice.core.dtos.OrderProducts;
import br.com.microservices.orchestrated.productvalidationservice.core.enums.ESagaStatus;
import br.com.microservices.orchestrated.productvalidationservice.core.models.Validation;
import br.com.microservices.orchestrated.productvalidationservice.core.producers.KafkaProducer;
import br.com.microservices.orchestrated.productvalidationservice.core.repository.ProductRepository;
import br.com.microservices.orchestrated.productvalidationservice.core.repository.ValidationRepository;
import br.com.microservices.orchestrated.productvalidationservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cglib.core.Local;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.productvalidationservice.core.enums.ESagaStatus.FAILED;
import static br.com.microservices.orchestrated.productvalidationservice.core.enums.ESagaStatus.ROLLBACK_PENDING;
import static br.com.microservices.orchestrated.productvalidationservice.core.enums.ESagaStatus.SUCCESS;
import static java.lang.Boolean.FALSE;
import static org.springframework.util.ObjectUtils.isEmpty;

@Service
@Slf4j
@AllArgsConstructor
public class ProductValidationService {

    private static final String CURRENT_SOURCE = "PRODUCT_VALIDATION_SERVICE";

    private final JsonUtil jsonUtil;

    private final KafkaProducer producer;

    private final ProductRepository productRepository;

    private final ValidationRepository validationRepository;

    public void validateExistingProducts(Event event) {

        try {

            checkCurrentValidation(event);

            createValidation(event, true);

            handleSuccess(event);

        } catch (Exception e) {

            log.error("Error trying do validate products: ", e);

            handleFailCurrentNotExecuted(event, e.getMessage());
        }

        producer.sendEvent(jsonUtil.toJson(event));
    }

    private void checkCurrentValidation(Event event) {

        validateProductsInformed(event);

        if (validationRepository.existsByOrderIdAndTransactionId(event.getOrderId(), event.getTransactionId())) {

            throw new ValidationException("Theres another transaction for this validation");
        }

        event.getPayload().getProducts().forEach(product -> {

            validateProductInformed(product);
            validateExistingProduct(product.getProduct().getCode());

        });
    }

    private void validateExistingProduct(String code) {

        if (!productRepository.existsByCode(code)) {

            throw new ValidationException("Product does not exists in database");
        }
    }

    private void validateProductInformed(OrderProducts product) {

        if (isEmpty(product.getProduct()) || isEmpty(product.getProduct().getCode())) {

            throw new ValidationException("Product must be informed");
        }
    }

    private void validateProductsInformed(Event event) {

        if (isEmpty(event.getPayload()) || isEmpty(event.getPayload().getProducts())) {

            throw new ValidationException("Product list is empty");
        }

        if (isEmpty(event.getPayload().getId()) || isEmpty(event.getPayload().getTransactionId())) {

            throw new ValidationException("OrderId and TransactionId must be informed");
        }
    }

    private void createValidation(Event event, Boolean success) {

        Validation validation = Validation.builder()
                .orderId(event.getOrderId())
                .transactionId(event.getTransactionId())
                .success(success)
                .build();

        validationRepository.save(validation);
    }

    private void handleSuccess(Event event) {

        event.setStatus(SUCCESS);
        event.setSource(CURRENT_SOURCE);

        addHistory(event, "Products are validated with success");

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

    private void handleFailCurrentNotExecuted(Event event, String message) {

        event.setStatus(ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);

        addHistory(event, "Fail to validate products: " + message);
    }

    public void rollbackEvent(Event event) {

        changeValidationToFail(event);

        event.setStatus(FAILED);
        event.setSource(CURRENT_SOURCE);

        addHistory(event, "Rollback executed for product-validation");

        producer.sendEvent(jsonUtil.toJson(event));
    }

    private void changeValidationToFail(Event event) {

        this.validationRepository.findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
                .ifPresentOrElse(validation -> {
                    validation.setSuccess(FALSE);
                    this.validationRepository.save(validation);
                }, () -> createValidation(event, FALSE));
    }
}
