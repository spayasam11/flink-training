package org.apache.flink.training.assignments.domain;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;
import java.util.Objects;

@JsonSerialize
public class FlatSymbolOrder extends IncomingEvent {
    private static final long serialVersionUID = 7946678872780554209L;
    private String orderId;
    private String cusip;
    private String assetType;
    private BuySell buySell;
    private BigDecimal bidOffer;
    private String currency;
    private int quantity;
    private long orderTime;
    private String account;
    private String subAccount;

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getSubAccount() {
        return subAccount;
    }

    public void setSubAccount(String subAccount) {
        this.subAccount = subAccount;
    }


    public FlatSymbolOrder(String cusip, int quantity) {
        this.cusip = cusip;
        this.quantity = quantity;
        super.setTimestamp(System.currentTimeMillis()-1000);
    }

    public FlatSymbolOrder() {
    }

    public static FlatSymbolOrder.FlatOrderBuilder builder() {
        return new FlatSymbolOrder.FlatOrderBuilder();
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


    public String toString() {
        String var10000 = this.getOrderId();
        return "FlatSymbolOrder( cusip="+ this.getCusip() +  " quantity=" + this.getQuantity() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlatSymbolOrder order = (FlatSymbolOrder) o;
        return quantity == order.quantity &&
                orderTime == order.orderTime &&
                Objects.equals(orderId, order.orderId) &&
                Objects.equals(cusip, order.cusip) &&
                Objects.equals(assetType, order.assetType) &&
                buySell == order.buySell &&
                Objects.equals(bidOffer, order.bidOffer) &&
                Objects.equals(currency, order.currency) &&
                Objects.equals(account, order.account) &&
                Objects.equals(subAccount, order.subAccount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, cusip, assetType, buySell, bidOffer, currency, quantity, orderTime);
    }

    protected boolean canEqual(Object other) {
        return other instanceof FlatSymbolOrder;
    }


    public static class FlatOrderBuilder {
        private String account;
        private String cusip;
        private String subAccount;
        private int quantity;
        private long orderTime;

        FlatOrderBuilder() {
        }


        public FlatSymbolOrder.FlatOrderBuilder cusip(String cusip) {
            this.cusip = cusip;
            return this;
        }


        public FlatSymbolOrder.FlatOrderBuilder quantity(int quantity) {
            this.quantity = quantity;
            return this;
        }

        public FlatSymbolOrder build() {
            return new FlatSymbolOrder(this.cusip,  this.quantity);
        }

        public String toString() {
            return "";
        }
    }
}

