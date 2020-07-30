package org.apache.flink.training.assignments.sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.function.BiConsumer;

public class LogSink<T> implements SinkFunction<T>, Serializable {

    private final Logger LOG;
    // NOTE: tempting to use a BiConsumer here but won't work because Java functions are not easily serializable
    private final LoggerEnum logger;
    private final String marker;

    public LogSink(Logger LOG, LoggerEnum logger, String marker) {
        this.LOG = LOG;
        this.logger = logger;
        this.marker = marker;
    }

    @Override
    public void invoke(T value) {
        // Again a BiConsumer would be way cooler but won't work without a flink custom serializer
        switch (logger) {
            case INFO:
                LOG.info(marker, value.toString());
                return;
            case ERROR:
                LOG.error(marker, value.toString());
                return;
            case TRACE:
                LOG.trace(marker, value.toString());
                return;
            case DEBUG:
                LOG.debug(marker, value.toString());
                return;
        }
    }

    public void invoke(BiConsumer<String, String> logger, T value) {
        logger.accept(marker, value.toString());
    }

    public enum LoggerEnum {INFO, ERROR, TRACE, DEBUG}

}
