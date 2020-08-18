package org.apache.flink.training.assignments.domain;

import java.io.Serializable;
import java.util.Objects;

public class Allocation implements Serializable {
    private static final long serialVersionUID = -887981985593847911L;
    private String subAccount;
    private String account;

    public Allocation(String subAccount, String account, String orderId, String orderSize, int quantity) {
        this.subAccount = subAccount;
        this.account = account;
        this.orderId = orderId;
        this.orderSize = orderSize;
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Allocation that = (Allocation) o;
        return quantity == that.quantity &&
                Objects.equals(subAccount, that.subAccount) &&
                Objects.equals(account, that.account) &&
                Objects.equals(orderId, that.orderId) &&
                Objects.equals(orderSize, that.orderSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subAccount, account, orderId, orderSize, quantity);
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    private String orderId;
    private String orderSize;
    private int quantity;

    public void setSubAccount(String subAccount) {
        this.subAccount = subAccount;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setOrderSize(String orderSize) {
        this.orderSize = orderSize;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getOrderSize() {
        return orderSize;
    }

    public Allocation(String subAccount, int quantity) {
        this.subAccount = subAccount;
        this.quantity = quantity;
    }

    public Allocation() {
    }

    public static Allocation.AllocationBuilder builder() {
        return new Allocation.AllocationBuilder();
    }

    public String getSubAccount() {
        return this.subAccount;
    }


    public int getQuantity() {
        return this.quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String toString() {
        String var10000 = this.getSubAccount().toString();
        return "Allocation(account=" + var10000 + ", subAccount=" + this.getSubAccount() + ", quantity=" + this.getQuantity() + ")";
    }

    protected boolean canEqual(Object other) {
        return other instanceof Allocation;
    }

    public static class AllocationBuilder {
        private String subAccount;
        private int quantity;

        AllocationBuilder() {
        }



        public Allocation.AllocationBuilder subAccount(String subAccount) {
            this.subAccount = subAccount;
            return this;
        }

        public Allocation.AllocationBuilder quantity(int quantity) {
            this.quantity = quantity;
            return this;
        }

        public Allocation build() {
            return new Allocation( this.subAccount, this.quantity);
        }

        public String toString() {
            return "";
        }
    }
}
