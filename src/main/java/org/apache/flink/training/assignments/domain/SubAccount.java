package org.apache.flink.training.assignments.domain;

import java.util.Objects;

public class SubAccount {

    private String account;
    private String subAccount;
    private String benchmark;
    private String composedKey;

    public SubAccount() {
    }

    @Override
    public String toString() {
        return "Account{" +
                "accountName='" + account + '\'' +
                ", subAccountName='" + subAccount + '\'' +
                ", benchmark='" + benchmark + '\'' +
                ", composedKey='" + composedKey + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubAccount subAccount = (SubAccount) o;
        return Objects.equals(this.account, subAccount.account) &&
                Objects.equals(this.subAccount, subAccount.subAccount) &&
                Objects.equals(benchmark, subAccount.benchmark) &&
                Objects.equals(composedKey, subAccount.composedKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, subAccount, benchmark, composedKey);
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

    public String getBenchmark() {
        return benchmark;
    }

    public void setBenchmark(String benchmark) {
        this.benchmark = benchmark;
    }

    public String getComposedKey() {
        return composedKey;
    }

    public void setComposedKey(String composedKey) {
        this.composedKey = composedKey;
    }
}
