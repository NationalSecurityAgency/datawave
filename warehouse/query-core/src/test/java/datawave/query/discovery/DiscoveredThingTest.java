package datawave.query.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hadoop.io.MapWritable;
import org.junit.jupiter.api.Test;

public class DiscoveredThingTest {
    @Test
    public void testDiscoveredThingSimpleEqualityTest() {
        DiscoveredThing thing1 = new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable());
        DiscoveredThing thing2 = new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable());
        assertEquals(thing1, thing2);

        DiscoveredThing thing3 = new DiscoveredThing("", "", "", "", "", 0L, new MapWritable());
        DiscoveredThing thing4 = new DiscoveredThing("", "", "", "", "", 0L, new MapWritable());
        assertEquals(thing3, thing4);

        DiscoveredThing thing5 = new DiscoveredThing("", "", "", "", "", 0L, null);
        DiscoveredThing thing6 = new DiscoveredThing("", "", "", "", "", 0L, null);
        assertEquals(thing5, thing6);

    }

    @Test
    public void testDiscoveredThingSimpleInequalityTest() {
        DiscoveredThing thing1 = new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable());
        DiscoveredThing thing2 = new DiscoveredThing("bbc", "NETWORK", "csv", "20130102", "FOO", 240L, new MapWritable());
        assertNotEquals(thing1, thing2);

        DiscoveredThing thing3 = new DiscoveredThing("", "wanda", "", "", "", 0L, new MapWritable());
        DiscoveredThing thing4 = new DiscoveredThing("", "panda", "", "", "", 0L, new MapWritable());
        assertNotEquals(thing3, thing4);

        DiscoveredThing thing5 = new DiscoveredThing("", "wands", "", "", "", 0L, null);
        DiscoveredThing thing6 = new DiscoveredThing("", "wands", "", "", "", 0L, new MapWritable());
        assertNotEquals(thing5, thing6);

        DiscoveredThing thing7 = new DiscoveredThing("", "wanda", "", "", "", 0L, null);
        DiscoveredThing thing8 = new DiscoveredThing("", "panda", "", "", "", 0L, null);
        assertNotEquals(thing7, thing8);
    }
}
