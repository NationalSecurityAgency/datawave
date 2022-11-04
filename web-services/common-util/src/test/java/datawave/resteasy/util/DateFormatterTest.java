package datawave.resteasy.util;

import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DateFormatterTest {
    
    private DateFormatter formatter = new DateFormatter(null);
    
    @Test
    public void testDateFormatterFail1() {
        assertThrows(RuntimeException.class, () -> formatter.fromString("20120101120000"));
    }
    
    @Test
    public void testDateFormatterFail2() {
        assertThrows(RuntimeException.class, () -> formatter.fromString("20120101 250000"));
    }
    
    @Test
    public void testDateFormatterSuccess() {
        SimpleDateFormat f = new SimpleDateFormat(DateFormatter.getFormatPattern());
        Date d = formatter.fromString("20120101 131145");
        assertEquals("20120101 131145.000", f.format(d));
    }
}
