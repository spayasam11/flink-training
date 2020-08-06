package org.apache.flink.training.assignments.domain;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

@JsonSerialize
public class FlatOrder extends IncomingEvent {
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


    public FlatOrder( String cusip, int quantity, String account, String subAccount) {
        this.cusip = cusip;
        this.quantity = quantity;
        this.account = account;
        this.subAccount = subAccount;
        super.setTimestamp(System.currentTimeMillis()-1000);
    }

    public FlatOrder() {
    }

    public static FlatOrder.FlatOrderBuilder builder() {
        return new FlatOrder.FlatOrderBuilder();
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
        return "FlatOrder( cusip="+ this.getCusip() +  " quantity=" + this.getQuantity() + ", account=" + this.getAccount() + " subAccount=  " + this.getSubAccount() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlatOrder order = (FlatOrder) o;
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
        return other instanceof FlatOrder;
    }


    public static class FlatOrderBuilder {
        private String account;
        private String cusip;
        private String subAccount;
        private int quantity;
        private long orderTime;

        FlatOrderBuilder() {
        }


        public FlatOrder.FlatOrderBuilder cusip(String cusip) {
            this.cusip = cusip;
            return this;
        }


        public FlatOrder.FlatOrderBuilder quantity(int quantity) {
            this.quantity = quantity;
            return this;
        }

        public FlatOrder build() {
            return new FlatOrder(this.cusip,  this.quantity, this.account,this.subAccount);
        }

        public String toString() {
            return "";
        }
    }
}

