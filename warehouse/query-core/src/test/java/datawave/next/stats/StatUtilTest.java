package datawave.next.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class StatUtilTest {

    @Test
    public void testFormatNanos() {
        assertEquals("0ns []", StatUtil.formatNanos(123L));
        assertEquals("1ms [1ms]", StatUtil.formatNanos(1234567L));
        assertEquals("1s 234ms [1234ms]", StatUtil.formatNanos(1234567890L));
    }

}
