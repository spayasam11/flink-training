package org.apache.flink.training.assignments.domain;

public abstract class IncomingEvent implements Event {
    private static final long serialVersionUID = 3291021245765477246L;
    private long timestamp;

    public IncomingEvent() {
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String toString() {
        return "IncomingEvent(timestamp=" + this.getTimestamp() + ")";
    }
}
