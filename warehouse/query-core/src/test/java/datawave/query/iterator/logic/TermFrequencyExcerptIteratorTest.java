package datawave.query.iterator.logic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.protobuf.TermWeight;
import datawave.query.Constants;
import datawave.query.iterator.SortedListKeyValueIterator;

@RunWith(EasyMockRunner.class)
public class TermFrequencyExcerptIteratorTest extends EasyMockSupport {

    private static final Text row = new Text("20220115_1");
    private static final Text colf = ExtendedDataTypeHandler.TERM_FREQUENCY_COLUMN_FAMILY;

    @Mock
    private IteratorEnvironment env;
    private static final List<Map.Entry<Key,Value>> source = new ArrayList<>();
    private final Map<String,String> options = new HashMap<>();
    private final TermFrequencyExcerptIterator iterator = new TermFrequencyExcerptIterator();

    @BeforeClass
    public static void beforeClass() throws Exception {
        givenData("email", "123.456.789", "BODY", "the quick brown fox jumped over the lazy dog ");
        givenData("email", "123.456.789", "CONTENT", "there is no greater divide in fandoms than that between star wars and star trek fans");
        givenData("scan", "987.654.321", "TITLE", "document scan 12345");
        givenData("scan", "987.654.321", "CONTENT", "we've been trying to reach you about your car warranty");
    }

    private static void givenData(String datatype, String uid, String fieldName, String phrase) {
        Multimap<String,Integer> termIndexes = getIndexes(phrase);
        for (String term : termIndexes.keySet()) {
            NormalizedFieldAndValue nfv = new NormalizedFieldAndValue(fieldName, term);
            Text colq = new Text(datatype + Constants.NULL + uid + Constants.NULL + nfv.getIndexedFieldValue() + Constants.NULL + nfv.getIndexedFieldName());
            // @formatter:off
            TermWeight.Info info = TermWeight.Info.newBuilder()
                            .addAllTermOffset(termIndexes.get(term))
                            .addScore(10000000)
                            .addPrevSkips(0)
                            .setZeroOffsetMatch(true)
                            .build();
            // @formatter:on
            Key key = new Key(row, colf, colq, new ColumnVisibility("ALL"), new Date().getTime());
            Value value = new Value(info.toByteArray());
            Map.Entry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
            source.add(entry);
        }
    }

    private static Multimap<String,Integer> getIndexes(String phrase) {
        String[] terms = phrase.split(" ");
        Multimap<String,Integer> map = ArrayListMultimap.create();
        for (int i = 0; i < terms.length; i++) {
            map.put(terms[i], i);
        }
        return map;
    }

    @After
    public void tearDown() throws Exception {
        options.clear();
    }

    /**
     * Verify that the expected phrase is found for the typical usage case of this iterator.
     */
    @Test
    public void testMatchFound() throws IOException {
        givenOptions("BODY", 1, 5);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "123.456.789"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("BODY" + Constants.NULL + "quick brown fox jumped"), topKey.getColumnQualifier());
    }

    /**
     * Verify that specifying offsets outside the bounds of an actual phrase results in the full phrase being returned without a trailing space.
     */
    @Test
    public void testOffsetRangeOutsideBounds() throws IOException {
        givenOptions("CONTENT", -1, 20);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "123.456.789"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "there is no greater divide in fandoms than that between star wars and star trek fans"),
                        topKey.getColumnQualifier());
    }

    /**
     * Verify that specifying matching offsets results in a blank excerpt.
     */
    @Test
    public void testMatchingStartAndEndOffset() throws IOException {
        givenOptions("CONTENT", 2, 2);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "123.456.789"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL), topKey.getColumnQualifier());
    }

    /**
     * Verify that a non-matching field for a matching datatype and uid results in a blank excerpt returned.
     */
    @Test
    public void testNoMatchFoundForField() throws IOException {
        givenOptions("BAD_FIELD", 1, 5);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "123.456.789"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("BAD_FIELD" + Constants.NULL), topKey.getColumnQualifier());
    }

    /**
     * Verify that specifying a non-matching datatype or uid results in the iterator not having a top.
     */
    @Test
    public void testNoMatchFoundForDataTypeAndUid() throws IOException {
        givenOptions("BODY", 1, 5);
        initIterator();

        Key startKey = new Key(row, new Text("other" + Constants.NULL + "111.111.111"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.seek(range, Collections.emptyList(), false);

        assertFalse(iterator.hasTop());
    }

    /**
     * Verify that an exception is thrown when validating a start offset than is greater than the end offset.
     */
    @Test
    public void testStartOffsetGreaterThanEndOffset() {
        givenOptions("BODY", 10, 1);

        Assert.assertThrows("End offset must be greater than start offset", IllegalArgumentException.class, () -> iterator.validateOptions(options));
    }

    private void givenOptions(String field, int start, int end) {
        options.put(TermFrequencyExcerptIterator.FIELD_NAME, field);
        options.put(TermFrequencyExcerptIterator.START_OFFSET, String.valueOf(start));
        options.put(TermFrequencyExcerptIterator.END_OFFSET, String.valueOf(end));
    }

    private void initIterator() throws IOException {
        // noinspection unchecked
        iterator.init(new SortedListKeyValueIterator(source), options, env);
    }
}
