package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.utils.MyOrders;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class AssignmentGroupBy implements AggregateFunction<Order, List<Order>, MyOrders> {

    @Override
    public List<Order> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Order> add(
            Order order,
            List<Order> orders) {
        orders.add(order);
        return orders;
    }

    @Override
    public MyOrders getResult(List<Order> orders) {
        return new MyOrders(orders, LocalDateTime.now());
    }

    @Override
    public List<Order> merge(List<Order> orders,
                                    List<Order> acc1) {
        orders.addAll(acc1);
        return orders;
    }
}