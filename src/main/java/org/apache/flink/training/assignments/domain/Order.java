package org.apache.flink.training.assignments.domain;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
@JsonSerialize
public class Order extends IncomingEvent {
    private static final long serialVersionUID = 7946678872780554209L;
    private String orderId;
    private String cusip;
    private String assetType;
    private BuySell buySell;
    private BigDecimal bidOffer;
    private String currency;
    private int quantity;
    private long orderTime;
    private List<Allocation> allocations;

    public Order(String orderId, String cusip, String assetType, BuySell buySell, BigDecimal bidOffer, String currency, int quantity, long orderTime, List<Allocation> allocations) {
        this.orderId = orderId;
        this.cusip = cusip;
        this.assetType = assetType;
        this.buySell = buySell;
        this.bidOffer = bidOffer;
        this.currency = currency;
        this.quantity = quantity;
        this.orderTime = orderTime;
        this.allocations = allocations;
        super.setTimestamp(System.currentTimeMillis()-5000);
    }

    public Order() {
    }

    public static Order.OrderBuilder builder() {
        return new Order.OrderBuilder();
    }

    public byte[] key() {
        return this.orderId.getBytes();
    }

    public String getOrderId() {
        return this.orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCusip() {
        return this.cusip;
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }

    public String getAssetType() {
        return this.assetType;
    }

    public void setAssetType(String assetType) {
        this.assetType = assetType;
    }

    public BuySell getBuySell() {
        return this.buySell;
    }

    public void setBuySell(BuySell buySell) {
        this.buySell = buySell;
    }

    public BigDecimal getBidOffer() {
        return this.bidOffer;
    }

    public void setBidOffer(BigDecimal bidOffer) {
        this.bidOffer = bidOffer;
    }

    public String getCurrency() {
        return this.currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public int getQuantity() {
        return this.quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public long getOrderTime() {
        return this.orderTime;
    }

    public void setOrderTime(long orderTime) {
        this.orderTime = orderTime;
    }

    public List<Allocation> getAllocations() {
        return this.allocations;
    }

    public void setAllocations(List<Allocation> allocations) {
        this.allocations = allocations;
    }

    public String toString() {
        String var10000 = this.getOrderId();
        return "Order(orderId=" + var10000 + ", cusip=" + this.getCusip() + ", assetType=" + this.getAssetType() + ", buySell=" + this.getBuySell() + ", bidOffer=" + this.getBidOffer() + ", currency=" + this.getCurrency() + ", quantity=" + this.getQuantity() + ", orderTime=" + this.getOrderTime() + ", allocations=" + this.getAllocations() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return quantity == order.quantity &&
                orderTime == order.orderTime &&
                Objects.equals(orderId, order.orderId) &&
                Objects.equals(cusip, order.cusip) &&
                Objects.equals(assetType, order.assetType) &&
                buySell == order.buySell &&
                Objects.equals(bidOffer, order.bidOffer) &&
                Objects.equals(currency, order.currency) &&
                Objects.equals(allocations, order.allocations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, cusip, assetType, buySell, bidOffer, currency, quantity, orderTime, allocations);
    }

    protected boolean canEqual(Object other) {
        return other instanceof Order;
    }


    public static class OrderBuilder {
        private String orderId;
        private String cusip;
        private String assetType;
        private BuySell buySell;
        private BigDecimal bidOffer;
        private String currency;
        private int quantity;
        private long orderTime;
        private List<Allocation> allocations;

        OrderBuilder() {
        }

        public Order.OrderBuilder orderId(String orderId) {
            this.orderId = orderId;
            return this;
        }

        public Order.OrderBuilder cusip(String cusip) {
            this.cusip = cusip;
            return this;
        }

        public Order.OrderBuilder assetType(String assetType) {
            this.assetType = assetType;
            return this;
        }

        public Order.OrderBuilder buySell(BuySell buySell) {
            this.buySell = buySell;
            return this;
        }

        public Order.OrderBuilder bidOffer(BigDecimal bidOffer) {
            this.bidOffer = bidOffer;
            return this;
        }

        public Order.OrderBuilder currency(String currency) {
            this.currency = currency;
            return this;
        }

        public Order.OrderBuilder quantity(int quantity) {
            this.quantity = quantity;
            return this;
        }

        public Order.OrderBuilder orderTime(long orderTime) {
            this.orderTime = orderTime;
            return this;
        }

        public Order.OrderBuilder allocations(List<Allocation> allocations) {
            this.allocations = allocations;
            return this;
        }

        public Order build() {
            return new Order(this.orderId, this.cusip, this.assetType, this.buySell, this.bidOffer, this.currency, this.quantity, this.orderTime, this.allocations);
        }

        public String toString() {
            return "Order.OrderBuilder(orderId=" + this.orderId + ", cusip=" + this.cusip + ", assetType=" + this.assetType + ", buySell=" + this.buySell + ", bidOffer=" + this.bidOffer + ", currency=" + this.currency + ", quantity=" + this.quantity + ", orderTime=" + this.orderTime + ", allocations=" + this.allocations + ")";
        }
    }
}

