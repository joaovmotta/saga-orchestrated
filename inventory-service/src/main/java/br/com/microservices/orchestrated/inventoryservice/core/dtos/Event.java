package br.com.microservices.orchestrated.inventoryservice.core.dtos;


import br.com.microservices.orchestrated.inventoryservice.core.enums.ESagaStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.ObjectUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Event {

    private String id;

    private String transactionId;

    private String orderId;

    private Order payload;

    private String source;

    private ESagaStatus status;

    private List<History> eventHistory;

    private LocalDateTime createdAt;

    public void addToHistory(History history){

        if(ObjectUtils.isEmpty(this.eventHistory)){

            this.eventHistory = new ArrayList<>();
        }

        eventHistory.add(history);
    }
}
