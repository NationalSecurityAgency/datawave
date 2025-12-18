package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Test cases for testing the creation of a {@link QueryLimiter} via bean configurations.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration()
class QueryLimiterSpringTest {

    @Autowired
    private QueryLimiter limiter;
    
    @Test
    void testCreation() {
        assertThat(limiter).isNotNull();
        assertThat(limiter.getZookeeperConfig()).isEqualTo("localhost:2181");

        QueryLimitConfiguration config = limiter.getConfiguration();
        assertThat(config).isNotNull();

        assertThat(config.getDefaultUserQueryLimit()).isEqualTo(100);
        assertThat(config.getDefaultSystemQueryLimit()).isEqualTo(5000);
        assertThat(config.getInternalCacheMaxSize()).isEqualTo(300);

        List<UserLimitConfiguration> expectedUserConfigs = new ArrayList<>();
        expectedUserConfigs.add(new UserLimitConfiguration("cn=userA c=us", 250, Map.of("GROUP_1", 25, "GROUP_2", 75)));
        expectedUserConfigs.add(new UserLimitConfiguration("cn=userB c=us", 50, null));
        expectedUserConfigs.add(new UserLimitConfiguration("cn=userC c=us", null, Map.of("GROUP_1", 5)));
        assertThat(config.getUserConfigs()).isEqualTo(expectedUserConfigs);

        List<SystemLimitConfiguration> expectedSystemConfigs = new ArrayList<>();
        expectedSystemConfigs.add(new SystemLimitConfiguration("SYSTEM_.*", null, 500, Map.of("GROUP_1", 10)));
        expectedSystemConfigs.add(new SystemLimitConfiguration("SYSTEM_FOO", false, null, Map.of("GROUP_2", 200)));
        assertThat(config.getSystemConfigs()).isEqualTo(expectedSystemConfigs);

        List<QueryLogicGroupLimitConfiguration> expectedGroupConfigs = new ArrayList<>();
        expectedGroupConfigs.add(new QueryLogicGroupLimitConfiguration("GROUP_1", "LOGIC1.*", 50));
        expectedGroupConfigs.add(new QueryLogicGroupLimitConfiguration("GROUP_2", "LOGIC2.*", 25));
        assertThat(config.getQueryLogicGroupConfigs()).isEqualTo(expectedGroupConfigs);

        assertThat(limiter.getUserLimitProvider()).isNotNull();
        assertThat(limiter.getSystemLimitProvider()).isNotNull();
        assertThat(limiter.getQueryLogicGroupLimitProvider()).isNotNull();
    }

    @Configuration
    @ImportResource(locations = "classpath:TestQueryLimiterFactory.xml")
    public static class Config {

        /**
         * Provide a {@link QueryHeartbeatCache} required for injection into the {@link QueryLimiter} instance.
         */
        @Bean
        QueryHeartbeatCache queryHeartbeatCache() {
            return new QueryHeartbeatCache();
        }
    }
}
