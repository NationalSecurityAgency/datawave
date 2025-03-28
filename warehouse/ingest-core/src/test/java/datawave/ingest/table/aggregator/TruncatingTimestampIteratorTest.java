package datawave.ingest.table.aggregator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.Test;

import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.util.CompositeTimestamp;

public class TruncatingTimestampIteratorTest {

    @Test
    public void testBeginingOfDay() {
        long eventDate1 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-01-01T00:00:00Z")).toEpochMilli();
        long eventDate2 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-01-01T23:59:59Z")).toEpochMilli();
        for (long i = eventDate1; i <= eventDate2; i += 7) {
            assertEquals(eventDate1, TruncatingTimestampIterator.beginningOfDay(i));
        }
    }

    @Test
    public void testTimestampTruncation() {
        long eventDate1 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-02-02T00:00:00Z")).toEpochMilli();
        long eventDate2 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-02-02T23:59:59Z")).toEpochMilli();
        for (long i = eventDate1; i <= eventDate2; i += 11) {
            assertEquals(eventDate1, TruncatingTimestampIterator.truncateTimestamp(i));
        }

        long composite1 = CompositeTimestamp.getCompositeDeltaTimeStamp(eventDate1, 20);
        long composite2 = CompositeTimestamp.getCompositeDeltaTimeStamp(eventDate2, 20);
        for (long i = composite1; i <= composite2; i += 11) {
            assertEquals(composite1, TruncatingTimestampIterator.truncateTimestamp(i));
        }
    }

    @Test
    public void testKeyTruncation() {
        long eventDate1 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-03-03T00:00:00Z")).toEpochMilli();
        long eventDate2 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-03-03T23:59:59Z")).toEpochMilli();
        Key expected = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), eventDate1, false);
        for (long i = eventDate1; i <= eventDate2; i += 41) {
            Key key = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), i, false);
            assertEquals(expected, TruncatingTimestampIterator.getTruncatedTimestampKey(key));
        }

        long composite1 = CompositeTimestamp.getCompositeDeltaTimeStamp(eventDate1, 20);
        long composite2 = CompositeTimestamp.getCompositeDeltaTimeStamp(eventDate2, 20);
        expected = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), composite1, false);
        for (long i = composite1; i <= composite2; i += 13) {
            Key key = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), i, false);
            assertEquals(expected, TruncatingTimestampIterator.getTruncatedTimestampKey(key));
        }
    }

    @Test
    public void testIterator() throws IOException {
        long eventDate1 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-03-03T00:00:00Z")).toEpochMilli();
        long eventDate2 = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2020-03-03T23:59:59Z")).toEpochMilli();
        Key expected = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), eventDate1, false);
        TreeMap<Key,Value> keys = new TreeMap<>();
        for (long i = eventDate1; i <= eventDate2; i += 317) {
            Key key = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), i, false);
            keys.put(key, new Value(String.valueOf(i).getBytes()));
        }
        SortedKeyValueIterator<Key,Value> source = new SortedListKeyValueIterator(keys);
        SortedKeyValueIterator<Key,Value> iterator = new TruncatingTimestampIterator(source);
        iterator.seek(new Range(), new ArrayList(), false);
        while (iterator.hasTop()) {
            Key key = iterator.getTopKey();
            assertEquals(expected, key);
            assertEquals(keys.get(source.getTopKey()), iterator.getTopValue());
            iterator.next();
        }

        long composite1 = CompositeTimestamp.getCompositeDeltaTimeStamp(eventDate1, 20);
        long composite2 = CompositeTimestamp.getCompositeDeltaTimeStamp(eventDate2, 20);
        expected = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), composite1, false);
        keys.clear();
        for (long i = composite1; i <= composite2; i += 317) {
            Key key = new Key("r".getBytes(), "cf".getBytes(), "cq".getBytes(), "v".getBytes(), i, false);
            keys.put(key, new Value(String.valueOf(i).getBytes()));
        }
        source = new SortedListKeyValueIterator(keys);
        iterator = new TruncatingTimestampIterator(source);
        iterator.seek(new Range(), new ArrayList(), false);
        while (iterator.hasTop()) {
            Key key = iterator.getTopKey();
            assertEquals(expected, key);
            assertEquals(keys.get(source.getTopKey()), iterator.getTopValue());
            iterator.next();
        }
    }
}
