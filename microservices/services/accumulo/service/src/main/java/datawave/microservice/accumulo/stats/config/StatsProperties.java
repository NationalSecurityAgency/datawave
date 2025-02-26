package datawave.microservice.accumulo.stats.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "accumulo.stats")
public class StatsProperties {
    
    private String monitorStatsUriTemplate = "http://%s/rest/xml";
    
    public void setMonitorStatsUriTemplate(String monitorStatsUriTemplate) {
        this.monitorStatsUriTemplate = monitorStatsUriTemplate;
    }
    
    public String getMonitorStatsUriTemplate() {
        return monitorStatsUriTemplate;
    }
}
