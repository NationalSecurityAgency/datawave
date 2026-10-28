package datawave.webservice.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dw.test-datawave-user-service")
public class TestDatawaveUserServiceProperties {
    private List<TestDatawaveUserProperties> testUsers = new ArrayList<>();

    public List<TestDatawaveUserProperties> getTestUsers() {
        return testUsers;
    }

    public void setTestUsers(List<TestDatawaveUserProperties> testUsers) {
        this.testUsers = testUsers;
    }
}
