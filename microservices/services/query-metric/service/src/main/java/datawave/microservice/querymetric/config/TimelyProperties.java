package datawave.microservice.querymetric.config;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.validation.constraints.AssertTrue;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.query.metric.timely")
public class TimelyProperties {

    private boolean enabled = false;
    private String host = null;
    private Protocol protocol = null;
    private int port = 4242;
    private Map<String,String> tags = new LinkedHashMap<>();

    public enum Protocol {
        TCP, UDP
    }

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

    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public Map<String,String> getTags() {
        return tags;
    }

    public void setTags(Map<String,String> tags) {
        this.tags = tags;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @AssertTrue(message = "host must not be blank when Timely is enabled")
    public boolean isHostValid() {
        return !enabled || StringUtils.isNotBlank(host);
    }

    @AssertTrue(message = "port must be between 1 and 65535 when Timely is enabled")
    public boolean isPortValid() {
        return !enabled || (port >= 1 && port <= 65535);
    }

    @AssertTrue(message = "protocol must be set when Timely is enabled")
    public boolean isProtocolValid() {
        return !enabled || protocol != null;
    }
}
