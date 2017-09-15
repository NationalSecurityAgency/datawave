package datawave.microservice.config.web;

import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "server", ignoreInvalidFields = true)
public class DatawaveServerProperties extends ServerProperties {
    private Integer securePort;
    private boolean nonSecureEnabled;
    
    public Integer getSecurePort() {
        return securePort;
    }
    
    public void setSecurePort(int securePort) {
        this.securePort = securePort;
    }
    
    public boolean isNonSecureEnabled() {
        return nonSecureEnabled;
    }
    
    public void setNonSecureEnabled(boolean nonSecureEnabled) {
        this.nonSecureEnabled = nonSecureEnabled;
    }
}
