package datawave.query.pointer;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.easymock.EasyMockSupport;
import org.geotools.util.Base64;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Multimap;

import datawave.attribute.pointer.DataPointer;
import datawave.attribute.pointer.ViewDataPointer;

public class ViewDataPointerHandlerTest extends EasyMockSupport {
    private ViewDataPointerHandler handler;
    private SortedKeyValueIterator<Key,Value> source;

    @Before
    public void setup() {
        source = createMock(SortedKeyValueIterator.class);
        handler = new ViewDataPointerHandler(Collections.EMPTY_SET, -1, "truncateMe");
    }

    @Test(expected = IllegalStateException.class)
    public void uninitializedFetchTest() throws IOException {
        replayAll();

        handler.fetch(new SampleDataPointer(), new Key());

        verifyAll();
    }

    @Test
    public void badPointerFetchTest() throws IOException {
        replayAll();

        handler.init(source, Collections.emptyMap(), null);
        Multimap<Key,Value> result = handler.fetch(new SampleDataPointer(), new Key());
        assertTrue(result.isEmpty());

        verifyAll();
    }

    @Test
    public void unmatchedFieldTest() throws IOException {
        replayAll();

        handler.init(source, Collections.emptyMap(), null);
        Multimap<Key,Value> result = handler.fetch(new ViewDataPointer("someShard", "someDataType", "1.2.3", "someView"),
                        new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000'));
        assertTrue(result.isEmpty());

        verifyAll();
    }

    @Test
    public void nonExistentPointerTest() throws IOException {
        ViewDataPointer dataPointer = new ViewDataPointer("someShard", "someDataType", "1.2.3", "someView");
        Key start = dataPointer.get();
        source.seek(eq(new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false)), eq(Collections.emptyList()), eq(false));
        expect(source.hasTop()).andReturn(false);

        replayAll();

        handler = new ViewDataPointerHandler(Set.of("someField"));
        handler.init(source, Collections.emptyMap(), null);
        Multimap<Key,Value> result = handler.fetch(dataPointer, new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000'));
        assertTrue(result.isEmpty());

        verifyAll();
    }

    @Test
    public void singlePointerTest() throws IOException {
        ViewDataPointer dataPointer = new ViewDataPointer("someShard", "someDataType", "1.2.3", "someView");
        Key start = dataPointer.get();
        source.seek(eq(new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false)), eq(Collections.emptyList()), eq(false));
        expect(source.hasTop()).andReturn(true);
        // apply a visibility and timestamp that must translate to the final event key even though it has no timestamp or vis set
        expect(source.getTopKey()).andReturn(new Key("", "", "", "vis", 1234));
        expect(source.getTopValue()).andReturn(new Value(Base64.encodeBytes("this was the content of the pointer".getBytes(StandardCharsets.UTF_8))))
                        .anyTimes();
        source.next();
        expect(source.hasTop()).andReturn(false);

        replayAll();

        handler = new ViewDataPointerHandler(Set.of("someField"));
        handler.init(source, Collections.emptyMap(), null);
        Multimap<Key,Value> result = handler.fetch(dataPointer, new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000'));
        assertFalse(result.isEmpty());
        assertEquals(1, result.entries().size());
        Map.Entry<Key,Value> entry = result.entries().iterator().next();
        assertEquals(new Value(), entry.getValue());
        assertEquals(new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000' + "this was the content of the pointer", "vis", 1234),
                        entry.getKey());

        verifyAll();
    }

    @Test
    public void singleTruncatedPointerTest() throws IOException {
        ViewDataPointer dataPointer = new ViewDataPointer("someShard", "someDataType", "1.2.3", "someView");
        Key start = dataPointer.get();
        source.seek(eq(new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false)), eq(Collections.emptyList()), eq(false));
        expect(source.hasTop()).andReturn(true);
        // apply a visibility and timestamp that must translate to the final event key even though it has no timestamp or vis set
        expect(source.getTopKey()).andReturn(new Key("", "", "", "vis", 1234));
        expect(source.getTopValue()).andReturn(new Value(Base64.encodeBytes("this was the content of the pointer".getBytes(StandardCharsets.UTF_8))))
                        .anyTimes();
        source.next();
        expect(source.hasTop()).andReturn(false);

        replayAll();

        handler = new ViewDataPointerHandler(Set.of("someField"), 6);
        handler.init(source, Collections.emptyMap(), null);
        Multimap<Key,Value> result = handler.fetch(dataPointer, new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000'));
        assertFalse(result.isEmpty());
        assertEquals(1, result.entries().size());
        Map.Entry<Key,Value> entry = result.entries().iterator().next();
        assertEquals(new Value(), entry.getValue());
        assertEquals(new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000' + "this w", "vis", 1234), entry.getKey());

        verifyAll();
    }

    @Test
    public void singleTruncatedPointerWithFieldTest() throws IOException {
        ViewDataPointer dataPointer = new ViewDataPointer("someShard", "someDataType", "1.2.3", "someView");
        Key start = dataPointer.get();
        source.seek(eq(new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false)), eq(Collections.emptyList()), eq(false));
        expect(source.hasTop()).andReturn(true);
        // apply a visibility and timestamp that must translate to the final event key even though it has no timestamp or vis set
        expect(source.getTopKey()).andReturn(new Key("", "", "", "vis", 1234));
        expect(source.getTopValue()).andReturn(new Value(Base64.encodeBytes("this was the content of the pointer".getBytes(StandardCharsets.UTF_8))))
                        .anyTimes();
        source.next();
        expect(source.hasTop()).andReturn(false);

        replayAll();

        handler = new ViewDataPointerHandler(Set.of("someField"), 6, "TRUNCATED_DATA");
        handler.init(source, Collections.emptyMap(), null);
        Multimap<Key,Value> result = handler.fetch(dataPointer, new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000'));
        assertFalse(result.isEmpty());
        assertEquals(2, result.entries().size());
        Collection<Value> values = result.get(new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "someField" + '\u0000' + "this w", "vis", 1234));
        assertFalse(values.isEmpty());
        assertEquals(1, values.size());
        assertEquals(new Value(), values.iterator().next());
        values = result.get(new Key("someShard", "someDataType" + '\u0000' + "1.2.3", "TRUNCATED_DATA" + '\u0000' + "someField"));
        assertFalse(values.isEmpty());
        assertEquals(1, values.size());
        assertEquals(new Value(), values.iterator().next());

        verifyAll();
    }

    private static final class SampleDataPointer implements DataPointer {
        @Override
        public Key get() {
            return null;
        }
    }
}
