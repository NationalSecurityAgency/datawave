package datawave.next.stats;

import java.io.Serializable;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Meta stats for candidate retrieval
 */
public class DocumentRetrievalStats implements Serializable {

    private static final long serialVersionUID = 7847751715708664481L;

    private static final Logger log = LoggerFactory.getLogger(DocumentRetrievalStats.class);

    private final int window = 10_000;

    private final DescriptiveStatistics submitStats = new DescriptiveStatistics(window);
    private final DescriptiveStatistics scanStats = new DescriptiveStatistics(window);
    private final DescriptiveStatistics elapsedStats = new DescriptiveStatistics(window);

    private String slowestContext = null;
    private long slowestTime = 0L;

    public synchronized void merge(ScanTimeStats stats) {
        this.submitStats.addValue(stats.getSubmitTime());
        this.scanStats.addValue(stats.getScanTime());
        this.elapsedStats.addValue(stats.getElapsedTime());

        if (stats.getScanTime() > slowestTime) {
            slowestTime = stats.getScanTime();
            slowestContext = stats.getContext();
        }
    }

    public String getSubmitStats() {
        return getStats(submitStats);
    }

    public String getScanStats() {
        return getStats(scanStats);
    }

    public String getElapsedStats() {
        return getStats(elapsedStats);
    }

    public String getStats(DescriptiveStatistics stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("min: ").append(format(stats.getMin()));
        sb.append(", avg: ").append(format(stats.getMean()));
        sb.append(", max: ").append(format(stats.getMax()));

        sb.append(", p50: ").append(format(stats.getPercentile(50)));
        sb.append(", p95: ").append(format(stats.getPercentile(95)));
        sb.append(", p99: ").append(format(stats.getPercentile(99)));
        return sb.toString();
    }

    public String getSlowestScan() {
        return format(slowestTime) + " ms for record id: " + slowestContext;
    }

    private String format(double ns) {
        return StatUtil.formatNanos((long) ns);
    }
}
