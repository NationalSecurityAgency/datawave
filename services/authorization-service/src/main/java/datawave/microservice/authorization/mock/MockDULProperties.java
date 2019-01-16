package datawave.microservice.authorization.mock;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@EnableConfigurationProperties(MockDULProperties.class)
@ConfigurationProperties(prefix = "mock.users")
public class MockDULProperties {
    private String serverDnRegex;
    private Map<String,String> globalRolesToAuths = new HashMap<>();
    private Map<String,Map<String,String>> perUserRolesToAuths = new HashMap<>();
    
    public String getServerDnRegex() {
        return serverDnRegex;
    }
    
    public void setServerDnRegex(String serverDnRegex) {
        this.serverDnRegex = serverDnRegex;
    }
    
    public Map<String,String> getGlobalRolesToAuths() {
        return globalRolesToAuths;
    }
    
    public void setGlobalRolesToAuths(Map<String,String> globalRolesToAuths) {
        this.globalRolesToAuths = globalRolesToAuths;
    }
    
    public Map<String,Map<String,String>> getPerUserRolesToAuths() {
        return perUserRolesToAuths;
    }
    
    public void setPerUserRolesToAuths(Map<String,Map<String,String>> perUserRolesToAuths) {
        this.perUserRolesToAuths = perUserRolesToAuths;
    }
}
