package datawave.ingest.data.config.ingest;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;

public class WhindexConfigTest {

    @Test
    public void testGettersAndSetters() {
        WhindexConfig config = new WhindexConfig();
        config.setValueField("field1");
        config.setValues(Arrays.asList("val1", "val2"));
        config.setSourceField("src");
        config.setDestField("dst");
        config.setOverloaded(true);

        // Verify that the getter returns the value that was set
        assertEquals("field1", config.getValueField());
        assertEquals(Arrays.asList("val1", "val2"), config.getValues());
        assertEquals("src", config.getSourceField());
        assertEquals("dst", config.getDestField());
        assertTrue(config.isOverloaded());
    }

    @Test
    public void testEqualsAndHashCode() {
        WhindexConfig config1 = new WhindexConfig();
        config1.setValueField("field");
        config1.setValues(Arrays.asList("val1", "val2"));
        config1.setSourceField("src");
        config1.setDestField("dst");
        config1.setOverloaded(false);

        WhindexConfig config2 = new WhindexConfig();
        config2.setValueField("field");
        config2.setValues(Arrays.asList("val1", "val2"));
        config2.setSourceField("src");
        config2.setDestField("dst");
        config2.setOverloaded(false);

        // The two objects should be equal and have the same hash code.
        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());

        // Change one property in config2 and they should no longer be equal.
        config2.setOverloaded(true);
        assertNotEquals(config1, config2);
    }

    @Test
    public void testEqualsWithNullAndDifferentType() {
        WhindexConfig config = new WhindexConfig();
        config.setValueField("field");
        config.setValues(Arrays.asList("val1", "val2"));
        config.setSourceField("src");
        config.setDestField("dst");
        config.setOverloaded(false);

        // Equals should return false when compared to null.
        assertNotEquals(config, null);
        // Equals should return false when compared to an object of a different type.
        assertNotEquals(config, "spaghetti");

        // Verify that the object is equal to itself.
        assertEquals(config, config);
    }
}
