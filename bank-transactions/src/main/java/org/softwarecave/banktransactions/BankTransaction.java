package org.softwarecave.banktransactions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BankTransaction {
    private String clientName;
    private BigDecimal amount;
    private LocalDateTime transactionDateTime;
}

