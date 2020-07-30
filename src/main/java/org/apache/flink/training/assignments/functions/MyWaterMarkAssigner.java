package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Order;

import java.time.ZoneId;

public class MyWaterMarkAssigner implements AssignerWithPunctuatedWatermarks<Order> {

        public long extractTimestamp(Order element, long previousElementTimestamp) {
            return element.getTimestamp();// in mesecs.
        }

        public Watermark checkAndGetNextWatermark(Order lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp + 10000) ;//Elements that have timestamps lower than the watermark won't be processed at all.
        }
    }
