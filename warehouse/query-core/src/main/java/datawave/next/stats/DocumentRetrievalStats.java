package datawave.next.stats;

import java.text.DecimalFormat;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Meta stats for document scans (candidate retrieval)
 */
public class DocumentRetrievalStats {

    private static final Logger log = LoggerFactory.getLogger(DocumentRetrievalStats.class);

    private final int window = 10_000;

    private final DescriptiveStatistics submitStats = new DescriptiveStatistics(window);
    private final DescriptiveStatistics scanStats = new DescriptiveStatistics(window);
    private final DescriptiveStatistics elapsedStats = new DescriptiveStatistics(window);

    private static final DecimalFormat format = new DecimalFormat("#.#");

    public synchronized void merge(ScanTimeStats stats) {
        this.submitStats.addValue(stats.getSubmitTime());
        this.scanStats.addValue(stats.getScanTime());
        this.elapsedStats.addValue(stats.getElapsedTime());
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

    private String format(double ns) {
        ns /= 1_000_000;
        return format.format(ns);
    }
}
