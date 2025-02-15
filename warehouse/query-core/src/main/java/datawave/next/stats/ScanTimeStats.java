package datawave.next.stats;

/**
 * Timing stats for document retrievals
 */
public class ScanTimeStats {

    private long scanSubmitNanos = 0L;
    private long scanStartNanos = 0L;
    private long scanStopNanos = 0L;

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
