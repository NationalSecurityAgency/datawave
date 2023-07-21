package datawave.webservice.query.metric;

import java.util.HashSet;
import java.util.Set;

public class QueryMetricsWriterConfiguration {

    private String timelyHost = null;
    private int timelyPort = 0;
    private Set<String> timelyMetricTags = new HashSet<>();
    private boolean useRemoteService = false;

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
}
