package datawave.ingest.data.config.ingest;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
       Assertions.assertEquals("field1", config.getValueField());
       Assertions.assertEquals(Arrays.asList("val1", "val2"), config.getValues());
       Assertions.assertEquals("src", config.getSourceField());
       Assertions.assertEquals("dst", config.getDestField());
       Assertions.assertTrue(config.isOverloaded());
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
       Assertions.assertEquals(config1, config2);
       Assertions.assertEquals(config1.hashCode(), config2.hashCode());

        // Change one property in config2 and they should no longer be equal.
        config2.setOverloaded(true);
       Assertions.assertNotEquals(config1, config2);
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
       Assertions.assertNotEquals(config, null);
        // Equals should return false when compared to an object of a different type.
       Assertions.assertNotEquals(config, "spaghetti");

        // Verify that the object is equal to itself.
       Assertions.assertEquals(config, config);
    }
}
