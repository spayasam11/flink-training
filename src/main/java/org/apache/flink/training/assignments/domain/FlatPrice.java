package org.apache.flink.training.assignments.domain;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;
@JsonSerialize
public class FlatPrice {
    private static final long serialVersionUID = 7946678872780554209L;
    private String cusip;
    private BigDecimal price;

    public FlatPrice(String cusip, BigDecimal price) {
        this.cusip = cusip;
        this.price = price;
    }

    public String getCusip() {
        return cusip;
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

}
