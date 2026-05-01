package datawave.webservice.query.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ImmutableQueryLogicGroupLimitConfigurationTest {

    /**
     * Verify that an instance of {@link ImmutableQueryLogicGroupLimitConfiguration} created from a {@link QueryLogicGroupLimitConfiguration} is considered
     * equal to its mutable equivalent, has the same hashcode, and cannot be modified.
     */
    @Test
    void testImmutableCopy() {
        // Create the config to copy.
        QueryLogicGroupLimitConfiguration config = new QueryLogicGroupLimitConfiguration();
        config.setGroupName("EgdeGroupA");
        config.setQueryLimit(25);
        config.setQueryLogicPattern("Edge.*QueryLogic");

        // Create the immutable copy.
        ImmutableQueryLogicGroupLimitConfiguration immutable = new ImmutableQueryLogicGroupLimitConfiguration(config);

        // Verify they have the same hashcode.
        assertEquals(config.hashCode(), immutable.hashCode());

        // Verify the two configurations are considered equal.
        assertEquals(config, immutable);

        // Verify that the setter methods cannot be invoked.
        assertThrows(UnsupportedOperationException.class, () -> immutable.setGroupName("Other Name"));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setQueryLimit(10));
        assertThrows(UnsupportedOperationException.class, () -> immutable.setQueryLogicPattern("Event.*QueryLogic"));

        // Verify the string representations are equal other than the class name.
        String mutableString = config.toString();
        String immutableString = immutable.toString();

        assertTrue(mutableString.startsWith(QueryLogicGroupLimitConfiguration.class.getSimpleName()));
        assertTrue(immutableString.startsWith(ImmutableQueryLogicGroupLimitConfiguration.class.getSimpleName()));
        assertEquals(mutableString.substring(QueryLogicGroupLimitConfiguration.class.getSimpleName().length()),
                        immutableString.substring(ImmutableQueryLogicGroupLimitConfiguration.class.getSimpleName().length()));
    }
}
