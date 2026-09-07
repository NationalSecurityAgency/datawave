package datawave.ingest.data.config.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class WhindexConfigTest {

    /**
     * Tests that the builder and getters work correctly.
     * <p>
     * This test creates a new WhindexConfig via the builder, setting its properties, and then verifies that the getter methods return the expected values.
     * </p>
     */
    @Test
    public void testBuilderAndGetters() {
        WhindexConfig config = WhindexConfig.builder().withValueField("field1").withValues(Arrays.asList("val1", "val2")).withSourceField("src")
                        .withDestField("dst").withOverloaded(true).build();

        assertEquals("field1", config.getValueField());
        assertEquals(Arrays.asList("val1", "val2"), config.getValues());
        assertEquals("src", config.getSourceField());
        assertEquals("dst", config.getDestField());
        assertTrue(config.isOverloaded());
    }

    /**
     * Tests the equals() and hashCode() methods.
     * <p>
     * This test creates two WhindexConfig objects with the same properties via the builder and verifies equality/hashCode, then mutates one to ensure
     * inequality.
     * </p>
     */
    @Test
    public void testEqualsAndHashCode() {
        WhindexConfig config1 = WhindexConfig.builder().withValueField("field").withValues(Arrays.asList("val1", "val2")).withSourceField("src")
                        .withDestField("dst").withOverloaded(false).build();

        WhindexConfig config2 = WhindexConfig.builder().withValueField("field").withValues(Arrays.asList("val1", "val2")).withSourceField("src")
                        .withDestField("dst").withOverloaded(false).build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());

        // Different overloaded flag
        WhindexConfig config3 = WhindexConfig.builder().withValueField("field").withValues(Arrays.asList("val1", "val2")).withSourceField("src")
                        .withDestField("dst").withOverloaded(true).build();

        assertNotEquals(config1, config3);
    }

    /**
     * Tests equals() with null, different type, and self-comparison.
     */
    @Test
    public void testEqualsWithNullAndDifferentType() {
        WhindexConfig config = WhindexConfig.builder().withValueField("field").withValues(Arrays.asList("val1", "val2")).withSourceField("src")
                        .withDestField("dst").withOverloaded(false).build();

        assertNotEquals(null, config);
        assertNotEquals("spaghetti", config);
        assertEquals(config, config);
    }
}
