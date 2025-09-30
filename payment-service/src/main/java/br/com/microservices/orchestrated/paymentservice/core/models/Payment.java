package br.com.microservices.orchestrated.paymentservice.core.models;

import br.com.microservices.orchestrated.paymentservice.core.enums.EPaymentStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.paymentservice.core.enums.EPaymentStatus.PENDING;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Table(name = "PAYMENT")
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private String transactionId;

    @Column(nullable = false)
    private Integer totalItems;

    @Column(nullable = false)
    private Double totalAmount;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private EPaymentStatus status;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    public void prePersist(){

        LocalDateTime now = LocalDateTime.now();

        status = PENDING;
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    public void preUpdate(){

        LocalDateTime now = LocalDateTime.now();
        updatedAt = now;
    }
}
