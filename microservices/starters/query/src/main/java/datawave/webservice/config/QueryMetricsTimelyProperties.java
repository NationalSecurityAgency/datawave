package datawave.webservice.config;

import java.util.HashSet;
import java.util.Set;

public class QueryMetricsTimelyProperties {
    private String host = null;
    private int port = 0;
    private Set<String> metricFieldTags = new HashSet<>();

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Set<String> getMetricFieldTags() {
        return metricFieldTags;
    }

    public void setMetricFieldTags(Set<String> metricFieldTags) {
        this.metricFieldTags = metricFieldTags;
    }
}
