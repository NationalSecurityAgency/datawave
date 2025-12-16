package datawave.query.data.parsers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

public class DayIndexKeyTest {

    private final Key dayIndexKey = new Key("20220101\0value", "FIELD", "datatype");

    private final DayIndexKey parser = new DayIndexKey();

    @Test
    public void testParse() {
        parser.parse(dayIndexKey);
        assertNormalKey(parser);
    }

    private void assertNormalKey(KeyParser parser) {
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals("datatype", parser.getDatatype());
        assertInstanceOf(DayIndexKey.class, parser);
        assertEquals("20220101", ((DayIndexKey) parser).getShard());
    }
}
