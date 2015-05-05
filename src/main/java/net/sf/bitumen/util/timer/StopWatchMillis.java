package net.sf.bitumen.util.timer;

public class StopWatchMillis implements IStopWatch {

    private final long start;

    public StopWatchMillis() {
        start = System.currentTimeMillis();
    }

    public StopWatchMillis(long init) {
        start = init;
    }

    @Override
    public long elapsed() {
        return System.currentTimeMillis() - start;
    }

}
