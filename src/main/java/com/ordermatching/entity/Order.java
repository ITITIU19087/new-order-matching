package com.ordermatching.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Columns;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@NoArgsConstructor
@Entity
@Data
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "order_id")
    private Long id;

    private String UUID;
    private String status;
    private String side;
    private Double price;
    private Double quantity;

    @Column(name = "create_time")
    @CreationTimestamp
    private LocalDateTime orderTime;
    private boolean matched = false;
    private boolean isTop = false;

    @ManyToOne
    @JoinColumn(name = "trade_orders")
    private Trade trade;
}
