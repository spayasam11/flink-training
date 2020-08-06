package org.apache.flink.training.assignments.domain;

import java.util.Objects;

public class CompositeKey {
    public CompositeKey(String cusip, String account, String subAccount) {
        this.cusip = cusip;
        this.account = account;
        this.subAccount = subAccount;
    }

    protected String cusip;
    protected String account;
    protected String subAccount;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Objects.equals(cusip, that.cusip) &&
                Objects.equals(account, that.account) &&
                Objects.equals(subAccount, that.subAccount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cusip, account, subAccount);
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
}
