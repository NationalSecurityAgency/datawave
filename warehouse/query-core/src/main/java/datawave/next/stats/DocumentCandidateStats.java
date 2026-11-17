package datawave.next.stats;

import java.io.Serializable;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Meta stats for candidate search against the field index
 */
public class DocumentCandidateStats implements Serializable {

    private static final long serialVersionUID = 6008862471729801263L;

    private final int window = 10_000;

    private final DescriptiveStatistics submitStats = new DescriptiveStatistics(window);
    private final DescriptiveStatistics scanStats = new DescriptiveStatistics(window);
    private final DescriptiveStatistics retrievalStats = new DescriptiveStatistics(window);
    private final DescriptiveStatistics elapsedStats = new DescriptiveStatistics(window);

    private final DescriptiveStatistics candidates = new DescriptiveStatistics(window);

    public void merge(DocIdQueryIterStats stats) {
        submitStats.addValue(stats.getTotalInitTime());
        scanStats.addValue(stats.getTotalScanTime());
        retrievalStats.addValue(stats.getTotalRetrievalTime());
        elapsedStats.addValue(stats.getTotalElapsedTime());
        candidates.addValue(stats.getTotalDocumentIds());
    }

    public String getScanStats() {
        return getStats(scanStats);
    }

    public String getCandidatesStats() {
        return getStats(candidates);
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

    /**
     * Cast the double to a long to eliminate rounding, then take the string value
     *
     * @param ms
     *            the data in milliseconds
     * @return formatted millis
     */
    private String format(double ms) {
        return String.valueOf((long) ms);
    }
}
