package datawave.microservice.authorization.mock;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(MockDULProperties.class)
@ConfigurationProperties(prefix = "mock.users")
public class MockDULProperties {
    private String serverDnRegex;
    private Map<String,String> globalRolesToAuths = new HashMap<>();
    private Map<String,MockDatawaveUser> mockUsers = new HashMap<>();
    
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
    
    public void setMockUsers(Map<String,MockDatawaveUser> mockUsers) {
        this.mockUsers = mockUsers;
    }
    
    public Map<String,MockDatawaveUser> getMockUsers() {
        return mockUsers;
    }
}
