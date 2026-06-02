package datawave.query.model;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.junit.Test;

public class FieldMappingTest {

    /**
     * Verify that creating a forward mapping with a regular field name does not result in any exceptions.
     */
    @Test
    public void testForwardMappingWithPlainFieldName() {
        assertDoesNotThrow(() -> new FieldMapping("datatype", "DB_NAME", "FIELD_NAME", Direction.FORWARD, "ALL", Collections.emptySet()));
    }

    /**
     * Verify that creating a forward mapping with an invalid pattern for the field name results in an exception.
     */
    @Test
    public void testForwardMappingWithInvalidPatternFieldName() {
        assertThrows(IllegalArgumentException.class, () -> new FieldMapping("datatype", "[\\]", "FIELD_NAME", Direction.FORWARD, "ALL", Collections.emptySet()),
                        "Invalid regex pattern supplied for field name: [\\]");
    }

    /**
     * Verify that creating a forward mapping with a valid pattern for the field name does not result in an exception.
     */
    @Test
    public void testForwardMappingWithValidPatternFieldName() {
        assertDoesNotThrow(() -> new FieldMapping("datatype", "DB_NAME.*", "FIELD_NAME", Direction.FORWARD, "ALL", Collections.emptySet()));
    }
}
