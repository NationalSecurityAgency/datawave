package datawave.webservice.config;

import java.util.ArrayList;
import java.util.List;

public class QueryMetricsTimelyProperties {
    private String host = null;
    private int port = 0;
    private List<String> metricFieldTags = new ArrayList<>();

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

    public List<String> getMetricFieldTags() {
        return metricFieldTags;
    }

    public void setMetricFieldTags(List<String> metricFieldTags) {
        this.metricFieldTags = metricFieldTags;
    }
}
