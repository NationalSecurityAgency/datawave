package datawave.next.stats;

/**
 * Timing stats for document retrievals
 */
public class ScanTimeStats {

    private String context;

    private long scanSubmitNanos = 0L;
    private long scanStartNanos = 0L;
    private long scanStopNanos = 0L;

    public void setContext(String context) {
        this.context = context;
        this.context = this.context.replaceAll("\u0000", "\\\\x00");
    }

    public String getContext() {
        return context;
    }

    public void markSubmit() {
        this.scanSubmitNanos = System.nanoTime();
    }

    public void markStart() {
        this.scanStartNanos = System.nanoTime();
    }

    public void markStop() {
        this.scanStopNanos = System.nanoTime();
    }

    public long getElapsedTime() {
        return scanStopNanos - scanSubmitNanos;
    }

    public long getScanTime() {
        return scanStopNanos - scanStartNanos;
    }

    public long getSubmitTime() {
        return scanStartNanos - scanSubmitNanos;
    }
}
