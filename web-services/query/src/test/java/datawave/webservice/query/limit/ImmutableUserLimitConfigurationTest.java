package datawave.webservice.query.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class ImmutableUserLimitConfigurationTest {

    /**
     * Verify that an instance of {@link ImmutableUserLimitConfiguration} created from a {@link UserLimitConfiguration} is considered equal to its mutable
     * equivalent, has the same hashcode, and cannot be modified.
     */
    @Test
    void testImmutableCopy() {
        // Create the config to copy.
        UserLimitConfiguration config = new UserLimitConfiguration();
        config.setUserDn("CN=User A, C=US");
        config.setQueryLimit(25);
        config.setQueryLogicGroupLimits(Map.of("EdgeGroup.*", 20, "EventGroup", 50));

        // Create the immutable copy.
        ImmutableUserLimitConfiguration immutable = new ImmutableUserLimitConfiguration(config);

        // Verify they have the same hashcode.
        assertEquals(config.hashCode(), immutable.hashCode());

        // Verify the two configurations are considered equal.
        assertEquals(config, immutable);

        // Verify that the setter methods cannot be invoked.
        assertThrows(UnsupportedOperationException.class, () -> immutable.setUserDn("otherDn"));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setQueryLimit(10));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setQueryLogicGroupLimits(Map.of()));

        // Verify the string representations are equal other than the class name.
        String mutableString = config.toString();
        String immutableString = immutable.toString();

        assertTrue(mutableString.startsWith(UserLimitConfiguration.class.getSimpleName()));
        assertTrue(immutableString.startsWith(ImmutableUserLimitConfiguration.class.getSimpleName()));
        assertEquals(mutableString.substring(UserLimitConfiguration.class.getSimpleName().length()),
                        immutableString.substring(ImmutableUserLimitConfiguration.class.getSimpleName().length()));
    }
}
