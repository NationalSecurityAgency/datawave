package datawave.resteasy.util;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class DateFormatterTest {

    private DateFormatter formatter = new DateFormatter(null);

    @Test(expected = RuntimeException.class)
    public void testDateFormatterFail1() {
        formatter.fromString("20120101120000");
    }

    @Test(expected = RuntimeException.class)
    public void testDateFormatterFail2() {
        formatter.fromString("20120101 250000");
    }

    @Test
    public void testDateFormatterSuccess() {
        SimpleDateFormat f = new SimpleDateFormat(DateFormatter.getFormatPattern());
        Date d = formatter.fromString("20120101 131145");
        assertEquals("20120101 131145.000", f.format(d));
    }
}
