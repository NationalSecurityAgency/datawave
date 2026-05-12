package datawave.next.stats;

import java.time.Clock;

/**
 * Timing stats for document retrievals
 */
public class ScanTimeStats {

    private String context;

    private long scanSubmitMS = 0L;
    private long scanStartMS = 0L;
    private long scanStopMS = 0L;

    private final Clock clock = Clock.systemUTC();

    public void setContext(String context) {
        this.context = context;
        this.context = this.context.replaceAll("\u0000", "\\\\x00");
    }

    public String getContext() {
        return context;
    }

    public void markSubmit() {
        this.scanSubmitMS = clock.millis();
    }

    public void markStart() {
        this.scanStartMS = clock.millis();
    }

    public void markStop() {
        this.scanStopMS = clock.millis();
    }

    public long getElapsedTime() {
        return scanStopMS - scanSubmitMS;
    }

    public long getScanTime() {
        return scanStopMS - scanStartMS;
    }

    public long getSubmitTime() {
        return scanStartMS - scanSubmitMS;
    }
}
