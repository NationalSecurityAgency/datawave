package datawave.microservice.config.web;

import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "server", ignoreInvalidFields = true)
public class DatawaveServerProperties extends ServerProperties {
    private Integer nonSecurePort = null;
    
    public Integer getNonSecurePort() {
        return nonSecurePort;
    }
    
    public void setNonSecurePort(Integer nonSecurePort) {
        this.nonSecurePort = nonSecurePort;
    }
}
