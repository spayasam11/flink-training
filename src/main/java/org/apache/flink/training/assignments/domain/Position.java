package org.apache.flink.training.assignments.domain;

import java.math.BigDecimal;
import java.util.Objects;

public class Position {
      String cusip;
      String account;
      String subAccount;
      int quantity;
    BigDecimal mktVal;
      int orderId;

    public Position(String cusip, String account, String subAccount, int quantity, BigDecimal mktVal, int orderId) {
        this.cusip = cusip;
        this.account = account;
        this.subAccount = subAccount;
        this.quantity = quantity;
        this.mktVal = mktVal;
        this.orderId = orderId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Position position = (Position) o;
        return quantity == position.quantity &&
                orderId == position.orderId &&
                Objects.equals(cusip, position.cusip) &&
                Objects.equals(account, position.account) &&
                Objects.equals(subAccount, position.subAccount) &&
                Objects.equals(mktVal, position.mktVal);
    }

    @Override
    public String toString() {
        return "Position{" +
                "cusip='" + cusip + '\'' +
                ", account='" + account + '\'' +
                ", subAccount='" + subAccount + '\'' +
                ", quantity=" + quantity +
                ", mktVal=" + mktVal +
                ", orderId=" + orderId +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(cusip, account, subAccount, quantity, mktVal, orderId);
    }

    public String getCusip() {
        return cusip;
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }

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

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getMktVal() {
        return mktVal;
    }

    public void setMktVal(BigDecimal mktVal) {
        this.mktVal = mktVal;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }
}
