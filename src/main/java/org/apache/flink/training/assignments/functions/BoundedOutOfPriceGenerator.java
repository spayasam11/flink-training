package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.FlatOrder;
import org.apache.flink.training.assignments.domain.Price;

/**
 * This generator generates watermarks assuming that elements come out of order to a certain degree only.
 * The latest elements for a certain timestamp t will arrive at most n milliseconds after the earliest
 * elements for timestamp t.
 */
public class BoundedOutOfPriceGenerator implements AssignerWithPeriodicWatermarks<Price> {

    private final long maxOutOfOrderness = 35000; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Price element, long previousElementTimestamp) {
        long timestamp = System.currentTimeMillis()-5000;//element.getTimestamp();
        previousElementTimestamp = previousElementTimestamp-5000;
        //System.out.println("verify timestamp" + timestamp + " "+previousElementTimestamp);
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}