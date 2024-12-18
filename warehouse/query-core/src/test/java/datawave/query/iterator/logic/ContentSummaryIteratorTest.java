package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.ContentSummaryIterator.ONLY_SPECIFIED;
import static datawave.query.iterator.logic.ContentSummaryIterator.SUMMARY_SIZE;
import static datawave.query.iterator.logic.ContentSummaryIterator.VIEW_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.query.Constants;
import datawave.query.iterator.SortedListKeyValueIterator;

@RunWith(EasyMockRunner.class)
public class ContentSummaryIteratorTest extends EasyMockSupport {

    private static final Text row = new Text("20220115_1");
    private static final Text colf = new Text(ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY);

    @Mock
    private IteratorEnvironment env;
    private static final List<Map.Entry<Key,Value>> source = new ArrayList<>();
    private final Map<String,String> options = new HashMap<>();
    private final ContentSummaryIterator iterator = new ContentSummaryIterator();

    @BeforeClass
    public static void beforeClass() throws IOException {
        givenData("email", "123.456.789", "CONTENT1", "test content");
        givenData("email", "987.654.321", "CONTENT1", "test content two first");
        givenData("email", "987.654.321", "CONTENT2", "test content two second");
        givenData("pdf", "111.222.333", "CONTENT2", "this is a test of a longer content compared to the other ones to test trimming");
        givenData("pdf", "111.222.333", "CONTENT31", "test content wildcard matching one");
        givenData("pdf", "111.222.333", "CONTENT32", "test content wildcard matching two");
    }

    private static void givenData(String datatype, String uid, String contentName, String content) throws IOException {
        Text colq = new Text(datatype + Constants.NULL + uid + Constants.NULL + contentName);
        Key key = new Key(row, colf, colq, new ColumnVisibility("ALL"), new Date().getTime());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.max(content.getBytes().length / 2, 1024));
        final OutputStream b64s = Base64.getEncoder().wrap(bos);
        final GZIPOutputStream gzip = new GZIPOutputStream(b64s);
        gzip.write(content.getBytes());
        gzip.close();
        b64s.close();
        bos.close();
        Value value = new Value(bos.toByteArray());
        Map.Entry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        source.add(entry);
    }

    @After
    public void tearDown() {
        options.clear();
    }

    /**
     * @param contentNameList
     *            a comma separated list of content names in order
     * @param only
     *            if we only want to use the content names from the passed in contentNameList
     */
    private void givenOptions(String contentNameList, int summarySize, boolean only) {
        if (contentNameList != null) {
            options.put(VIEW_NAMES, contentNameList);
        }
        options.put(SUMMARY_SIZE, String.valueOf(summarySize));
        options.put(ONLY_SPECIFIED, String.valueOf(only));
    }

    private void initIterator() throws IOException {
        iterator.init(new SortedListKeyValueIterator(source), options, env);
    }

    @Test
    public void testMatchFound1() throws IOException {
        givenOptions("CONTENT1", 100, false);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "123.456.789"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT1: test content"), topKey.getColumnQualifier());
    }

    @Test
    public void testMatchFound2() throws IOException {
        givenOptions("CONTENT2", 100, false);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "987.654.321"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "987.654.321"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT2: test content two second"), topKey.getColumnQualifier());
    }

    @Test
    public void testMatchFoundSpecificContentNotFirstInList() throws IOException {
        givenOptions("CONTENT2", 100, false);
        iterator.setViewNameList(List.of("CONTENT1", "CONTENT2"));
        iterator.init(new SortedListKeyValueIterator(source), options, env);

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "987.654.321"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "987.654.321"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT2: test content two second"), topKey.getColumnQualifier());
    }

    @Test
    public void testMatchFoundWithTruncatedOutput() throws IOException {
        givenOptions("CONTENT2", 30, false);
        initIterator();

        Key startKey = new Key(row, new Text("pdf" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("pdf" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT2: this is a test of a longer con"), topKey.getColumnQualifier());
    }

    @Test
    public void testMatchFoundWithTruncatedMinimumOutput() throws IOException {
        givenOptions("CONTENT2", -87, false);
        initIterator();

        Key startKey = new Key(row, new Text("pdf" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("pdf" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT2: t"), topKey.getColumnQualifier());
    }

    @Test
    public void testMatchFoundWithSizeOverMax() throws IOException {
        givenOptions("CONTENT1", 9000, false);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "123.456.789"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT1: test content"), topKey.getColumnQualifier());
    }

    @Test
    public void testMatchFoundWithTrailingRegex() throws IOException {
        givenOptions("CONTENT3*", 100, false);
        initIterator();

        Key startKey = new Key(row, new Text("pdf" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("pdf" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT31: test content wildcard matching one\nCONTENT32: test content wildcard matching two"), topKey.getColumnQualifier());
    }

    @Test
    public void testNoMatchFoundForDataTypeAndUid() throws IOException {
        givenOptions("CONTENT2", 50, false);
        initIterator();

        Key startKey = new Key(row, new Text("other" + Constants.NULL + "111.111.111"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertFalse(iterator.hasTop());
    }

    @Test
    public void testNoMatchFoundForContentName() throws IOException {
        givenOptions("THISWONTBEFOUND", 100, true);
        iterator.setViewNameList(List.of("CONTENT1", "CONTENT2", "return", "of", "the", "mack"));
        iterator.init(new SortedListKeyValueIterator(source), options, env);

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "987.654.321"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertFalse(iterator.hasTop());
    }
}
