package datawave.test.iter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.accumulo.inmemory.util.SortedMapIterator;

/**
 * Demonstrates how to use {@link org.springframework.test.util.ReflectionTestUtils} to inject a {@link java.time.Clock} into the {@link ClockIterator} using
 * Mockito.
 */
public class ClockIteratorTest {

    private static final Value EMPTY_VALUE = new Value();

    private final Map<String,String> options = new HashMap<>();

    private final Clock clock = Mockito.mock(Clock.class);

    @BeforeEach
    public void beforeEach() {
        options.clear();

        // inject the mock clock and set initial time
        ReflectionTestUtils.setField(ClockIterator.class, "clock", clock);
        when(clock.millis()).thenReturn(0L);
    }

    private void givenAdvanceOnNext() {
        options.put(ClockIterator.ADVANCE_ON_NEXT, "true");
    }

    private void givenAdvanceOnSeek() {
        options.put(ClockIterator.ADVANCE_ON_SEEK, "true");
    }

    private void givenAdvanceOnBoth() {
        options.put(ClockIterator.ADVANCE_ON_BOTH, "true");
    }

    private void givenAdvanceAmount() {
        options.put(ClockIterator.ADVANCE_AMOUNT, "100");
    }

    @Test
    public void testNoAdvanceOptionSet() {
        Exception e = assertThrows(IllegalStateException.class, this::getIter);
        String expected = "At least one advance option must be specified";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testAdvanceOptionSetButNoAdvanceAmount() {
        givenAdvanceOnNext();
        Exception e = assertThrows(IllegalStateException.class, this::getIter);
        String expected = "ADVANCE_AMOUNT option must be specified";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testAdvanceOnNext() throws Exception {
        givenAdvanceOnNext();
        givenAdvanceAmount();
        assertEquals(0L, clock.millis());
        driveIterator();
        // 100ms per next call. One next call per element + one next call to determine no next element
        assertEquals(1100L, clock.millis());
    }

    @Test
    public void testAdvanceOnSeek() throws Exception {
        givenAdvanceOnSeek();
        givenAdvanceAmount();

        assertEquals(0L, clock.millis());
        driveIterator();
        // 100ms per seek call, seek only called once at the start
        assertEquals(100L, clock.millis());
    }

    @Test
    public void testAdvanceOnBoth() throws Exception {
        givenAdvanceOnBoth();
        givenAdvanceAmount();

        assertEquals(0L, clock.millis());
        driveIterator();
        // 100ms per next and seek. Eleven next calls + one seek call.
        assertEquals(1200L, clock.millis());
    }

    @Test
    public void testAdvanceOnSeekAndAdvanceOnNext() throws Exception {
        givenAdvanceOnNext();
        givenAdvanceOnSeek();
        givenAdvanceAmount();

        assertEquals(0L, clock.millis());
        driveIterator();
        // 100ms per next and seek. Eleven next calls + one seek call.
        assertEquals(1200L, clock.millis());
    }

    private void driveIterator() throws Exception {
        SortedKeyValueIterator<Key,Value> iter = getIter();
        while (iter.hasTop()) {
            assertNotNull(iter.getTopKey());
            assertNotNull(iter.getTopValue());
            iter.next();
        }
        assertFalse(iter.hasTop());
    }

    private SortedKeyValueIterator<Key,Value> getIter() throws Exception {
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int i = 0; i < 10; i++) {
            data.put(new Key("row-" + i), EMPTY_VALUE);
        }

        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);

        ClockIterator clockIterator = new ClockIterator();
        clockIterator.init(source, options, new TestIteratorEnv());
        clockIterator.seek(new Range(), Collections.emptySet(), false);
        return clockIterator;
    }

}
