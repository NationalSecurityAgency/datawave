package datawave.webservice.query.metric;

import java.util.HashSet;
import java.util.Set;

public class QueryMetricsWriterConfiguration {

    private String timelyHost = null;
    private int timelyPort = 0;
    private int batchSize = 100;
    private int maxQueueSize = 250000;
    private long maxLatencyMs = 5000;
    private long maxShutdownMs = 30000;
    private Set<String> timelyMetricTags = new HashSet<>();
    private boolean useRemoteService = false;
    private int remoteProcessorThreads = 4;

    public String getTimelyHost() {
        return timelyHost;
    }

    public void setTimelyHost(String timelyHost) {
        this.timelyHost = timelyHost;
    }

    public int getTimelyPort() {
        return timelyPort;
    }

    public void setTimelyPort(int timelyPort) {
        this.timelyPort = timelyPort;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxLatencyMs(long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }

    public long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public void setMaxShutdownMs(long maxShutdownMs) {
        this.maxShutdownMs = maxShutdownMs;
    }

    public long getMaxShutdownMs() {
        return maxShutdownMs;
    }

    public Set<String> getTimelyMetricTags() {
        return timelyMetricTags;
    }

    public void setTimelyMetricTags(Set<String> timelyMetricTags) {
        this.timelyMetricTags = timelyMetricTags;
    }

    public boolean getUseRemoteService() {
        return useRemoteService;
    }

    public void setUseRemoteService(boolean useRemoteService) {
        this.useRemoteService = useRemoteService;
    }

    public int getRemoteProcessorThreads() {
        return remoteProcessorThreads;
    }

    public void setRemoteProcessorThreads(int remoteProcessorThreads) {
        this.remoteProcessorThreads = remoteProcessorThreads;
    }
}
