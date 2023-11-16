package com.ordermatching.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.List;

@Entity
@Data
@NoArgsConstructor
public class Trade {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long tradeId;

    private String UUID;
    private String buyOrderUUID;
    private String sellOrderUUID;
    private double quantity;
    private double price;
    private boolean isUpdated = false;

    @OneToMany(mappedBy = "trade")
    private List<Order> orders;
}
