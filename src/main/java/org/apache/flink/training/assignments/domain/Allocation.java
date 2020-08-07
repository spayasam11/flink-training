package org.apache.flink.training.assignments.domain;

import java.io.Serializable;
import java.util.Objects;

public class Allocation implements Serializable {
    private static final long serialVersionUID = -887981985593847911L;
    private SubAccount subAccount;
    private int quantity;

    public Allocation(SubAccount subAccount, int quantity) {
        this.subAccount = subAccount;
        this.quantity = quantity;
    }

    public Allocation() {
    }

    public static Allocation.AllocationBuilder builder() {
        return new Allocation.AllocationBuilder();
    }

    public SubAccount getSubAccount() {
        return this.subAccount;
    }

    public void setSubAccount(SubAccount subAccount) {
        this.subAccount = subAccount;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Allocation that = (Allocation) o;
        return quantity == that.quantity &&
                Objects.equals(subAccount, that.subAccount) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subAccount,  quantity);
    }

    protected boolean canEqual(Object other) {
        return other instanceof Allocation;
    }

    public static class AllocationBuilder {
        private SubAccount subAccount;
        private int quantity;

        AllocationBuilder() {
        }



        public Allocation.AllocationBuilder subAccount(SubAccount subAccount) {
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
