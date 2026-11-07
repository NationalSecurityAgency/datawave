package datawave.iterators.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TimeZone;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ResourceLock(Resources.TIME_ZONE)
public class AgeOffFilterBaseTest {
    private final AgeOffFilterBase filter = new AgeOffFilterBase() {
        @Override
        protected byte[] getDateBytes(Key k, Value v) {
            return new byte[0];
        }
    };

    @ParameterizedTest
    @CsvSource({"UTC, 0", "UTC, 1", "UTC, 30", "UTC, 365", "UTC, -1", "UTC, -2", "America/New_York, 0", "America/New_York, 365", "Pacific/Kiritimati, 1",
            "Asia/Kathmandu, 1"})
    public void testCutoffIncludesExtraComparisonDay(String zoneName, int thresholdDays) {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zoneName));
            ZoneId zone = ZoneId.systemDefault();
            LocalDate expectedDay = LocalDate.now(zone).minusDays((long) thresholdDays + 1);
            assertEquals(Date.from(expectedDay.atStartOfDay(zone).toInstant()), filter.getCutoffDate(thresholdDays));
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    @ParameterizedTest
    @CsvSource({"UTC, 2024-02-29", "UTC, 2000-02-29", "UTC, 2100-03-01", "UTC, 2024-01-01", "UTC, 2024-04-30", "America/New_York, 2024-03-10",
            "America/New_York, 2024-03-11", "America/New_York, 2024-11-03", "America/New_York, 2024-11-04", "America/Sao_Paulo, 2018-11-04",
            "Pacific/Kiritimati, 2024-02-29"})
    public void testCutoffAtDateAndTimeZoneBoundaries(String zoneName, String cutoffDay) {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zoneName));
            ZoneId zone = ZoneId.systemDefault();
            LocalDate expectedDay = LocalDate.parse(cutoffDay);
            int thresholdDays = Math.toIntExact(ChronoUnit.DAYS.between(expectedDay, LocalDate.now(zone)) - 1);
            assertEquals(Date.from(expectedDay.atStartOfDay(zone).toInstant()), filter.getCutoffDate(thresholdDays));
        } finally {
            TimeZone.setDefault(previous);
        }
    }
}
