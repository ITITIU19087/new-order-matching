package com.ordermatching.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import javax.persistence.*;
import java.time.LocalDateTime;
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

    @Column(name = "create_time")
    @CreationTimestamp
    private LocalDateTime tradeTime;

    @OneToMany(mappedBy = "trade")
    private List<Order> orders;
}
