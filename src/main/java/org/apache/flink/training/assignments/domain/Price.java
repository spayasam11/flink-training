package org.apache.flink.training.assignments.domain;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;
import java.util.Objects;
@JsonSerialize
public class Price extends IncomingEvent {
//"timestamp":0,"eos":false,"id":"1596774531794002","cusip":"Cusip10","price":1.7932667190262903,"effectiveDateTime":0
    private static final long serialVersionUID = 1L;
    private String id;
    private String cusip;
    private BigDecimal price;
    private long effectiveDateTime;



    public  Price(String id, String cusip, BigDecimal price, long effectiveDateTime) {
        this.id = id;
        this.cusip = cusip;
        this.price = price;
        this.effectiveDateTime = effectiveDateTime;
        super.setTimestamp(System.currentTimeMillis()-1000);
    }

    public  Price() {
    }


    @Override
    public String toString() {
        return "Price{" +
                "id='" + id + '\'' +
                ", cusip='" + cusip + '\'' +
                ", price=" + price +
                ", effectiveDateTime=" + effectiveDateTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Price price1 = (Price) o;
        return effectiveDateTime == price1.effectiveDateTime &&
                Objects.equals(id, price1.id) &&
                Objects.equals(cusip, price1.cusip) &&
                Objects.equals(price, price1.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, cusip, price, effectiveDateTime);
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public long getEffectiveDateTime() {
        return effectiveDateTime;
    }

    public void setEffectiveDateTime(long effectiveDateTime) {
        this.effectiveDateTime = effectiveDateTime;
    }

    @Override
    public byte[] key() {
        return new byte[0];
    }
}

