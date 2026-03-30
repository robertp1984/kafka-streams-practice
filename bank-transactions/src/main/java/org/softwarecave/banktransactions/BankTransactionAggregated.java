package org.softwarecave.banktransactions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BankTransactionAggregated {
    private String clientName;
    private BigDecimal totalAmount;
    private long transactionCount;
    private LocalDateTime latestTransactionDateTime;
}
