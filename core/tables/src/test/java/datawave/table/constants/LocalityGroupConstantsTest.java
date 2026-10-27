package datawave.table.constants;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * These constants should not change very often, if at all.
 * <p>
 * In the event that these need to change please ensure all child modules are updated
 */
public class LocalityGroupConstantsTest {

    @Test
    public void testLocalityGroupNames() {
        assertEquals("fullcontent", LocalityGroupConstants.FULL_CONTENT_LOCALITY);
        assertEquals("termfrequency", LocalityGroupConstants.TERM_FREQUENCY_LOCALITY);
    }
}
