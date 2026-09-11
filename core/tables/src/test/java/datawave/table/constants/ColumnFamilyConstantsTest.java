package datawave.table.constants;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

/**
 * It may seem odd to have a unit test for a constant class. However, a failing unit test here is a hint that changes could affect child modules in an
 * unexpected way.
 * <p>
 * Thus, great care should be taken when modifying core constants.
 */
public class ColumnFamilyConstantsTest {

    @Test
    public void testConstantsAsString() {
        assertEquals("tf", ColumnFamilyConstants.TERM_FREQUENCY);
        assertEquals("d", ColumnFamilyConstants.FULL_CONTENT);
    }

    @Test
    public void testConstantsAsText() {
        assertEquals(new Text("tf"), ColumnFamilyConstants.TERM_FREQUENCY_TEXT);
        assertEquals(new Text("d"), ColumnFamilyConstants.FULL_CONTENT_TEXT);
    }
}
