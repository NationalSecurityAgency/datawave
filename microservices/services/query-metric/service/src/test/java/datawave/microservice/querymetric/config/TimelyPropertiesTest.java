package datawave.microservice.querymetric.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class TimelyPropertiesTest {
    private static final String PREFIX = "datawave.query.metric.timely.";

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

    @Test
    void disabledTimelyDoesNotRequireEndpoint() {
        assertStarts(PREFIX + "enabled=false");
    }

    @Test
    void enabledTimelyRequiresHost() {
        assertFails("host must not be blank", PREFIX + "enabled=true", PREFIX + "protocol=TCP");
        assertFails("host must not be blank", PREFIX + "enabled=true", PREFIX + "host=   ", PREFIX + "protocol=TCP");
    }

    @Test
    void enabledTimelyRequiresValidPort() {
        assertFails("port must be between 1 and 65535", PREFIX + "enabled=true", PREFIX + "host=localhost", PREFIX + "protocol=TCP",
                        PREFIX + "port=0");
        assertFails("port must be between 1 and 65535", PREFIX + "enabled=true", PREFIX + "host=localhost", PREFIX + "protocol=TCP",
                        PREFIX + "port=65536");
    }

    @Test
    void enabledTimelyRequiresProtocol() {
        assertFails("protocol must be set", PREFIX + "enabled=true", PREFIX + "host=localhost");
    }

    @Test
    void validTcpAndUdpEndpointsStart() {
        assertStarts(PREFIX + "enabled=true", PREFIX + "host=localhost", PREFIX + "port=4242", PREFIX + "protocol=TCP");
        assertStarts(PREFIX + "enabled=true", PREFIX + "host=localhost", PREFIX + "port=4242", PREFIX + "protocol=UDP");
    }

    private void assertStarts(String... properties) {
        contextRunner.withPropertyValues(properties).run(context -> assertNull(context.getStartupFailure()));
    }

    private void assertFails(String expectedMessage, String... properties) {
        contextRunner.withPropertyValues(properties).run(context -> {
            Throwable failure = context.getStartupFailure();
            assertNotNull(failure);
            assertTrue(messages(failure).contains(expectedMessage), messages(failure));
        });
    }

    private static String messages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            messages.append(cause.getMessage()).append('\n');
        }
        return messages.toString();
    }

    @Configuration
    @EnableConfigurationProperties(TimelyProperties.class)
    static class TestConfiguration {}
}
