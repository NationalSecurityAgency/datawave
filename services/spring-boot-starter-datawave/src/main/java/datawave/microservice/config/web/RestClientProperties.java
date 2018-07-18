package datawave.microservice.config.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "server", ignoreInvalidFields = true)
public class RestClientProperties {
    private int maxConnectionsTotal = 20;
    private int maxConnectionsPerRoute = 20;
    
    public int getMaxConnectionsTotal() {
        return maxConnectionsTotal;
    }
    
    public void setMaxConnectionsTotal(int maxConnectionsTotal) {
        this.maxConnectionsTotal = maxConnectionsTotal;
    }
    
    public int getMaxConnectionsPerRoute() {
        return maxConnectionsPerRoute;
    }
    
    public void setMaxConnectionsPerRoute(int maxConnectionsPerRoute) {
        this.maxConnectionsPerRoute = maxConnectionsPerRoute;
    }
}
