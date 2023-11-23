package com.ordermatching.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TradePrice {
    public TradePrice(Double openPrice, Double closePrice, Double maxPrice, Double minPrice) {
        this.openPrice = openPrice;
        this.closePrice = closePrice;
        this.maxPrice = maxPrice;
        this.minPrice = minPrice;
    }

    private Double openPrice;
    private Double closePrice;
    private Double maxPrice;
    private Double minPrice;

}
