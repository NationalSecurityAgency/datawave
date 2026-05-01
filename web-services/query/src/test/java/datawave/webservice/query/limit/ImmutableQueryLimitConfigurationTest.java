package datawave.webservice.query.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ImmutableQueryLimitConfigurationTest {

    /**
     * Verify that an instance of {@link ImmutableUserLimitConfiguration} created from a {@link UserLimitConfiguration} is considered equal to its mutable
     * equivalent, has the same hashcode, and cannot be modified.
     */
    @Test
    void testImmutableCopy() {
        // Create the config to copy.
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultUserQueryLimit(100);
        config.setDefaultSystemQueryLimit(1000);
        config.setInternalCacheMaxSize(200);

        UserLimitConfiguration userLimitConfig1 = new UserLimitConfiguration();
        userLimitConfig1.setUserDn("CN=User A, C=US");
        userLimitConfig1.setQueryLimit(25);
        userLimitConfig1.setQueryLogicGroupLimits(Map.of("EdgeGroup.*", 20, "EventGroup", 50));
        UserLimitConfiguration userLimitConfig2 = new UserLimitConfiguration();
        userLimitConfig2.setUserDn("CN=User B, C=US");
        userLimitConfig2.setQueryLimit(100);
        userLimitConfig2.setQueryLogicGroupLimits(Map.of("EdgeGroup.*", 15, "EventGroup", 25));
        config.setUserConfigs(List.of(userLimitConfig1, userLimitConfig2));

        SystemLimitConfiguration systemLimitConfig1 = new SystemLimitConfiguration();
        systemLimitConfig1.setSystemPattern("Artemis.*");
        systemLimitConfig1.setQueryLimit(2000);
        systemLimitConfig1.setCountsAgainstUserLimit(false);
        systemLimitConfig1.setQueryLogicGroupLimits(Map.of("EdgeGroup.*", 300, "EventGroup", 500));
        SystemLimitConfiguration systemLimitConfig2 = new SystemLimitConfiguration();
        systemLimitConfig2.setSystemPattern("Athena.*");
        systemLimitConfig2.setQueryLimit(1500);
        systemLimitConfig2.setCountsAgainstUserLimit(true);
        systemLimitConfig2.setQueryLogicGroupLimits(Map.of("EdgeGroup.*", 600, "EventGroup", 800));
        config.setSystemConfigs(List.of(systemLimitConfig1, systemLimitConfig2));

        QueryLogicGroupLimitConfiguration queryLogicGroupLimitConfig1 = new QueryLogicGroupLimitConfiguration();
        queryLogicGroupLimitConfig1.setGroupName("EgdeGroupA");
        queryLogicGroupLimitConfig1.setQueryLimit(25);
        queryLogicGroupLimitConfig1.setQueryLogicPattern("Edge.*QueryLogic");
        QueryLogicGroupLimitConfiguration queryLogicGroupLimitConfig2 = new QueryLogicGroupLimitConfiguration();
        queryLogicGroupLimitConfig2.setGroupName("EventGroupA");
        queryLogicGroupLimitConfig2.setQueryLimit(30);
        queryLogicGroupLimitConfig2.setQueryLogicPattern("Event.*QueryLogic");
        config.setQueryLogicGroupConfigs(List.of(queryLogicGroupLimitConfig1, queryLogicGroupLimitConfig2));

        // Create the immutable copy.
        ImmutableQueryLimitConfiguration immutable = new ImmutableQueryLimitConfiguration(config);

        // Verify they have the same hashcode.
        assertEquals(config.hashCode(), immutable.hashCode());

        // Verify the two configurations are considered equal.
        assertEquals(config, immutable);

        // Verify that the setter methods cannot be invoked.
        assertThrows(UnsupportedOperationException.class, () -> immutable.setDefaultUserQueryLimit(75));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setDefaultSystemQueryLimit(2000));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setInternalCacheMaxSize(300));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setUserConfigs(List.of()));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setSystemConfigs(List.of()));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setQueryLogicGroupConfigs(List.of()));

        // Verify the string representations are equal other than the class names.
        String mutableString = config.toString();
        String immutableString = immutable.toString();

        assertTrue(mutableString.startsWith(QueryLimitConfiguration.class.getSimpleName()));
        assertTrue(immutableString.startsWith(ImmutableQueryLimitConfiguration.class.getSimpleName()));
        assertEquals(mutableString, immutableString.replaceAll("Immutable", ""));
    }

}
