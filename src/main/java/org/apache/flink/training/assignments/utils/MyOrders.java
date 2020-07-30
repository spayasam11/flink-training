package org.apache.flink.training.assignments.utils;

import org.apache.flink.training.assignments.domain.Order;

import java.time.LocalDateTime;
import java.util.List;

public class MyOrders {
    List<Order> orders;

    public MyOrders(List<Order> orders,LocalDateTime myTimeStamp) {
        this.orders = orders;
        this.myTimeStamp = myTimeStamp;
    }

    public List<Order> getOrders() {
        return orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }

    public LocalDateTime getMyTimeStamp() {
        return myTimeStamp;
    }

    public void setMyTimeStamp(LocalDateTime myTimeStamp) {
        this.myTimeStamp = myTimeStamp;
    }

    LocalDateTime myTimeStamp;


}
