package net.sf.bitumen.util;

public interface ILatencyLogger<E> {

    public final ILatencyLogger<? extends Object> DUMMY_LATENCY_LOGGER = new ILatencyLogger<Object>() {
        public void logLatency(long durationMillis, Object event) {};
    };

    public void logLatency(long durationMillis, E event);

}
