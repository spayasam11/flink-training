package org.apache.flink.training.assignments.domain;

import java.io.Serializable;

public interface Event extends Serializable {
    long getTimestamp();

    void setTimestamp(long var1);

    byte[] key();
}

