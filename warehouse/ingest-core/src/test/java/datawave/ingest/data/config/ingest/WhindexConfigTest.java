package datawave.ingest.data.config.ingest;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WhindexConfigTest {

    /**
     * Tests that the getters and setters work correctly.
     * <p>
     * This test creates a new WhindexConfig instance, sets its properties, and then verifies that the getter methods return the expected values.
     * </p>
     */
    @Test
    public void testGettersAndSetters() {
        // Create a new configuration instance and set its properties.
        WhindexConfig config = new WhindexConfig();
        config.setValueField("field1");
        config.setValues(Arrays.asList("val1", "val2"));
        config.setSourceField("src");
        config.setDestField("dst");
        config.setOverloaded(true);

        // Verify that the getter returns the value that was set.
        Assertions.assertEquals("field1", config.getValueField());
        Assertions.assertEquals(Arrays.asList("val1", "val2"), config.getValues());
        Assertions.assertEquals("src", config.getSourceField());
        Assertions.assertEquals("dst", config.getDestField());
        Assertions.assertTrue(config.isOverloaded());
    }

    /**
     * Tests the equals() and hashCode() methods.
     * <p>
     * This test creates two WhindexConfig objects with the same properties and verifies:
     * <ul>
     * <li>They are equal via equals()</li>
     * <li>They produce the same hash code</li>
     * </ul>
     * Then it modifies a property in one of the objects and asserts that they are no longer equal.
     * </p>
     */
    @Test
    public void testEqualsAndHashCode() {
        // Create the first configuration object with specific properties.
        WhindexConfig config1 = new WhindexConfig();
        config1.setValueField("field");
        config1.setValues(Arrays.asList("val1", "val2"));
        config1.setSourceField("src");
        config1.setDestField("dst");
        config1.setOverloaded(false);

        // Create the second configuration object with identical properties.
        WhindexConfig config2 = new WhindexConfig();
        config2.setValueField("field");
        config2.setValues(Arrays.asList("val1", "val2"));
        config2.setSourceField("src");
        config2.setDestField("dst");
        config2.setOverloaded(false);

        // Verify that both objects are considered equal.
        Assertions.assertEquals(config1, config2);
        // Verify that both objects generate the same hash code.
        Assertions.assertEquals(config1.hashCode(), config2.hashCode());

        // Change one property in config2.
        config2.setOverloaded(true);
        // Verify that the objects are no longer equal after the change.
        Assertions.assertNotEquals(config1, config2);
    }

    /**
     * Tests the equals() method with null and objects of different types.
     * <p>
     * This test ensures that:
     * <ul>
     * <li>A WhindexConfig object is not equal to null.</li>
     * <li>A WhindexConfig object is not equal to an object of a different type.</li>
     * <li>A WhindexConfig object is equal to itself.</li>
     * </ul>
     * </p>
     */
    @Test
    public void testEqualsWithNullAndDifferentType() {
        // Create a configuration object with sample properties.
        WhindexConfig config = new WhindexConfig();
        config.setValueField("field");
        config.setValues(Arrays.asList("val1", "val2"));
        config.setSourceField("src");
        config.setDestField("dst");
        config.setOverloaded(false);

        // Equals should return false when compared to null.
        Assertions.assertNotEquals(config, null);
        // Equals should return false when compared to an object of a different type (e.g., a String).
        Assertions.assertNotEquals(config, "spaghetti");

        // Verify that the object is equal to itself.
        Assertions.assertEquals(config, config);
    }
}
