package datawave.util;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.junit.Test;
import org.locationtech.jts.util.Assert;

public class CompositeTimestampTest {

    @Test
    public void testConversion() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2022-10-26T01:00:00Z")).toEpochMilli();
        long ageOff = eventDate + CompositeTimestamp.MILLIS_PER_DAY;
        long expectedTS = 142404234355328L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.equals(expectedTS, compositeTS);
        Assert.isTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.isTrue(!CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.isTrue(!CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.equals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.equals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate + CompositeTimestamp.MILLIS_PER_DAY);

    }
}
