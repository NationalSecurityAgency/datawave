package datawave.webservice.query.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class ImmutableSystemLimitConfigurationTest {

    /**
     * Verify that an instance of {@link ImmutableSystemLimitConfiguration} created from a {@link SystemLimitConfiguration} is considered equal to its mutable
     * equivalent, has the same hashcode, and cannot be modified.
     */
    @Test
    void testImmutableCopy() {
        // Create the config to copy.
        SystemLimitConfiguration config = new SystemLimitConfiguration();
        config.setSystemPattern("Artemis.*");
        config.setQueryLimit(2000);
        config.setCountsAgainstUserLimit(false);
        config.setQueryLogicGroupLimits(Map.of("EdgeGroup.*", 300, "EventGroup", 500));

        // Create the immutable copy.
        ImmutableSystemLimitConfiguration immutable = new ImmutableSystemLimitConfiguration(config);

        // Verify they have the same hashcode.
        assertEquals(config.hashCode(), immutable.hashCode());

        // Verify the two configurations are considered equal.
        assertEquals(config, immutable);

        // Verify that the setter methods cannot be invoked.
        assertThrows(UnsupportedOperationException.class, () -> immutable.setSystemPattern("Athena.*"));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setQueryLimit(1500));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setCountsAgainstUserLimit(true));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setQueryLogicGroupLimits(Map.of()));

        // Verify the string representations are equal other than the class name.
        String mutableString = config.toString();
        String immutableString = immutable.toString();

        assertTrue(mutableString.startsWith(SystemLimitConfiguration.class.getSimpleName()));
        assertTrue(immutableString.startsWith(ImmutableSystemLimitConfiguration.class.getSimpleName()));
        assertEquals(mutableString.substring(SystemLimitConfiguration.class.getSimpleName().length()),
                        immutableString.substring(ImmutableSystemLimitConfiguration.class.getSimpleName().length()));
    }
}
