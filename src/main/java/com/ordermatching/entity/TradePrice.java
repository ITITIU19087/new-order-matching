package com.ordermatching.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TradePrice {
    public TradePrice(String interval, Double openPrice, Double closePrice, Double maxPrice, Double minPrice) {
        this.interval = interval;
        this.openPrice = openPrice;
        this.closePrice = closePrice;
        this.maxPrice = maxPrice;
        this.minPrice = minPrice;
    }

    private String interval;
    private Double openPrice;
    private Double closePrice;
    private Double maxPrice;
    private Double minPrice;

}
