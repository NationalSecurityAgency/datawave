package datawave.webservice.query.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class QueryLimiterUtilsTest {

    /**
     * Verify behavior for {@link QueryLimiterUtils#normalizeUserDn(String)}.
     */
    @Test
    public void testNormalizeUserDn() {
        assertNull(QueryLimiterUtils.normalizeUserDn(null));
        assertEquals("", QueryLimiterUtils.normalizeUserDn("   "));
        assertEquals("cn=user", QueryLimiterUtils.normalizeUserDn(" CN=User "));
    }

    /**
     * Verify behavior for {@link QueryLimiterUtils#normalizeSystem(String)}.
     */
    @Test
    public void testNormalizeSystem() {
        assertEquals(QueryLimiterUtils.EMPTY_SYSTEM_FROM, QueryLimiterUtils.normalizeSystem(null));
        assertEquals(QueryLimiterUtils.EMPTY_SYSTEM_FROM, QueryLimiterUtils.normalizeSystem("   "));
        assertEquals("System-01", QueryLimiterUtils.normalizeSystem(" System-01 "));
    }

    /**
     * Verify behavior for {@link QueryLimiterUtils#normalizeQueryLogic(String)}.
     */
    @Test
    public void testNormalizeQueryLogic() {
        assertNull(QueryLimiterUtils.normalizeQueryLogic(null));
        assertEquals("", QueryLimiterUtils.normalizeQueryLogic("   "));
        assertEquals("TLDQueryLogic", QueryLimiterUtils.normalizeQueryLogic(" TLDQueryLogic "));
    }
}
