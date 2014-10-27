package springer.util;

/**
 * Utility class for thread/concurrency related functions.
 *
 */
public final class ThreadUtil {

    /** Utility class, hence constructor need not be accessible from out of the class. */
    private ThreadUtil() {
        // do nothing
    }

    /**
     * Sleep for specified duration in milliseconds.
     * @param millis duration to sleep in milliseconds
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

}
