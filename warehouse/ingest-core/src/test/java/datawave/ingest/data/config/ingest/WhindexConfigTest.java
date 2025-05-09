package datawave.ingest.data.config.ingest;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WhindexConfigTest {

    /**
     * Tests that the builder and getters work correctly.
     * <p>
     * This test creates a new WhindexConfig via the builder, setting its properties,
     * and then verifies that the getter methods return the expected values.
     * </p>
     */
    @Test
    public void testBuilderAndGetters() {
        WhindexConfig config = WhindexConfig.builder()
                .withValueField("field1")
                .withValues(Arrays.asList("val1", "val2"))
                .withSourceField("src")
                .withDestField("dst")
                .withOverloaded(true)
                .build();

        Assertions.assertEquals("field1", config.getValueField());
        Assertions.assertEquals(Arrays.asList("val1", "val2"), config.getValues());
        Assertions.assertEquals("src", config.getSourceField());
        Assertions.assertEquals("dst", config.getDestField());
        Assertions.assertTrue(config.isOverloaded());
    }

    /**
     * Tests the equals() and hashCode() methods.
     * <p>
     * This test creates two WhindexConfig objects with the same properties via the builder
     * and verifies equality/hashCode, then mutates one to ensure inequality.
     * </p>
     */
    @Test
    public void testEqualsAndHashCode() {
        WhindexConfig config1 = WhindexConfig.builder()
                .withValueField("field")
                .withValues(Arrays.asList("val1", "val2"))
                .withSourceField("src")
                .withDestField("dst")
                .withOverloaded(false)
                .build();

        WhindexConfig config2 = WhindexConfig.builder()
                .withValueField("field")
                .withValues(Arrays.asList("val1", "val2"))
                .withSourceField("src")
                .withDestField("dst")
                .withOverloaded(false)
                .build();

        Assertions.assertEquals(config1, config2);
        Assertions.assertEquals(config1.hashCode(), config2.hashCode());

        // Different overloaded flag
        WhindexConfig config3 = WhindexConfig.builder()
                .withValueField("field")
                .withValues(Arrays.asList("val1", "val2"))
                .withSourceField("src")
                .withDestField("dst")
                .withOverloaded(true)
                .build();

        Assertions.assertNotEquals(config1, config3);
    }

    /**
     * Tests equals() with null, different type, and self-comparison.
     */
    @Test
    public void testEqualsWithNullAndDifferentType() {
        WhindexConfig config = WhindexConfig.builder()
                .withValueField("field")
                .withValues(Arrays.asList("val1", "val2"))
                .withSourceField("src")
                .withDestField("dst")
                .withOverloaded(false)
                .build();

        Assertions.assertNotEquals(null, config);
        Assertions.assertNotEquals("spaghetti", config);
        Assertions.assertEquals(config, config);
    }
}
