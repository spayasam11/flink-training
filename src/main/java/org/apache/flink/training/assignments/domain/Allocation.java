package org.apache.flink.training.assignments.domain;

import java.io.Serializable;
import java.util.Objects;

public class Allocation implements Serializable {
    private static final long serialVersionUID = -887981985593847911L;
    private String account;
    private String subAccount;
    private int quantity;

    public Allocation(String account, String subAccount, int quantity) {
        this.account = account;
        this.subAccount = subAccount;
        this.quantity = quantity;
    }

    public Allocation() {
    }

    public static Allocation.AllocationBuilder builder() {
        return new Allocation.AllocationBuilder();
    }

    public String getAccount() {
        return this.account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getSubAccount() {
        return this.subAccount;
    }

    public void setSubAccount(String subAccount) {
        this.subAccount = subAccount;
    }

    public int getQuantity() {
        return this.quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String toString() {
        String var10000 = this.getAccount();
        return "Allocation(account=" + var10000 + ", subAccount=" + this.getSubAccount() + ", quantity=" + this.getQuantity() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Allocation that = (Allocation) o;
        return quantity == that.quantity &&
                Objects.equals(account, that.account) &&
                Objects.equals(subAccount, that.subAccount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, subAccount, quantity);
    }

    protected boolean canEqual(Object other) {
        return other instanceof Allocation;
    }

    public static class AllocationBuilder {
        private String account;
        private String subAccount;
        private int quantity;

        AllocationBuilder() {
        }

        public Allocation.AllocationBuilder account(String account) {
            this.account = account;
            return this;
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
            return new Allocation(this.account, this.subAccount, this.quantity);
        }

        public String toString() {
            return "Allocation.AllocationBuilder(account=" + this.account + ", subAccount=" + this.subAccount + ", quantity=" + this.quantity + ")";
        }
    }
}
