package datawave.query.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class IndexFieldHoleTest {
    
    private static long A_DAY_MILLIS = 1000L * 60 * 60 * 24;
    private static long SOME_DELTA = new Random().nextLong() % A_DAY_MILLIS;
    private static long A_DAY_PLUS_SOME_DELTA = A_DAY_MILLIS + SOME_DELTA;
    
    @Test
    public void testGetterSetter() {
        List<Date> dates = new ArrayList<>();
        Instant start = Instant.parse("2011-12-03T10:15:30Z");
        Instant end = Instant.parse("2020-12-03T10:15:30Z");
        for (long time = start.toEpochMilli(); time < end.toEpochMilli(); time += A_DAY_PLUS_SOME_DELTA) {
            dates.add(new Date(time));
        }
        SortedSet<Pair<Date,Date>> expected = new TreeSet();
        List<Pair<Date,Date>> holes = new ArrayList<>();
        Iterator<Date> dateItr = dates.iterator();
        Date last = dateItr.next();
        while (dateItr.hasNext()) {
            Date first = last;
            last = dateItr.next();
            holes.add(Pair.of(first, last));
            expected.add(Pair.of(floor(first), ceil(last)));
        }
        SortedSet<Pair<Date,Date>> unexpected = new TreeSet<>(expected);
        unexpected.add(Pair.of(new Date(), new Date()));
        
        IndexFieldHole expectedHole = new IndexFieldHole("f", "d", expected);
        
        IndexFieldHole hole = new IndexFieldHole("f", "d", holes);
        assertEquals(expected, hole.getDateRanges());
        assertEquals("f", hole.getFieldName());
        assertEquals("d", hole.getDatatype());
        assertEquals(expectedHole, hole);
        assertEquals(expectedHole.hashCode(), hole.hashCode());
        
        IndexFieldHole unexpectedHole1 = new IndexFieldHole("f", "d", unexpected);
        IndexFieldHole unexpectedHole2 = new IndexFieldHole("fn", "d", expected);
        IndexFieldHole unexpectedHole3 = new IndexFieldHole("f", "dn", expected);
        assertNotEquals(unexpectedHole1, hole);
        assertNotEquals(unexpectedHole2, hole);
        assertNotEquals(unexpectedHole3, hole);
    }
    
    private Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    
    private Date floor(Date d) {
        calendar.setTime(d);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }
    
    private Date ceil(Date d) {
        calendar.setTime(d);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        return calendar.getTime();
    }
}
