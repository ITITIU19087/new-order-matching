package com.ordermatching.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

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

    @OneToMany(mappedBy = "trade")
    private List<Order> orders;
}
