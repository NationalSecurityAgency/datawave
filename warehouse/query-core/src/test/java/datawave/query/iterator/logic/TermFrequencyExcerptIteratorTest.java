package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.END_OFFSET;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.FIELD_NAME;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.START_OFFSET;
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
    public static void beforeClass() {
        givenData("email", "123.456.789", "BODY", "the quick brown fox jumped over the lazy dog ");
        givenData("email", "123.456.789", "CONTENT", "there is no greater divide in fandoms than that between star wars and star trek fans");
        givenData("scan", "987.654.321", "TITLE", "document scan 12345");
        givenData("scan", "987.654.321", "CONTENT", "we've been trying to reach you about your car warranty");
        givenData("email", "111.222.333", "BODY", "the coldest tale <eps> ever told");
        givenData("email", "111.222.333", "CONTENT", "somewhere far along <eps> the street they <eps> lost their soul <eps> to a person so mean");
        givenData("email", "333.222.111", "BODY", "we like to repeat stuff do not ask questions we like to repeat stuff");
        multiWordsAtOneOffsetBuilder();
    }

    private static void givenData(String datatype, String uid, String fieldName, String phrase) {
        Multimap<String,Integer> termIndexes = getIndexes(phrase);
        for (String term : termIndexes.keySet()) {
            NormalizedFieldAndValue nfv = new NormalizedFieldAndValue(fieldName, term);
            Text colq = new Text(datatype + Constants.NULL + uid + Constants.NULL + nfv.getIndexedFieldValue() + Constants.NULL + nfv.getIndexedFieldName());
            // @formatter:off
            TermWeight.Info info = TermWeight.Info.newBuilder()
                            .addAllTermOffset(termIndexes.get(term))
                            .addScore(-1)
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

    private static void multiWordsAtOneOffsetBuilder() {
        Multimap<String,Integer> termIndexes1 = getIndexes("one two three four five six seven eight nine ten");
        Multimap<String,Integer> termIndexes2 = getIndexes("uno dos tres quatro cinco seis siete ocho nueve diez");
        int[] scores1 = {1560219, 1412017, 1592973, 2114938, 2124947, 2165412, 1215740, 1126708, 1273153, 149462};
        int[] scores2 = {1222082, 1748249, 153222, 1257611, 1235987, 1687421, 1243801, 213722, 1600256, 2171307};
        List<int[]> scoreList = new ArrayList<>();
        scoreList.add(scores1);
        scoreList.add(scores2);
        ArrayList<Multimap<String,Integer>> indexesList = new ArrayList<>();
        indexesList.add(termIndexes1);
        indexesList.add(termIndexes2);
        int i;
        for (int j = 0; j < indexesList.size(); j++) {
            Multimap<String,Integer> indexes = indexesList.get(j);
            int[] scores = scoreList.get(j);
            i = 0;
            for (String term : indexes.keySet()) {
                NormalizedFieldAndValue nfv = new NormalizedFieldAndValue("BODY", term);
                Text colq = new Text("email" + Constants.NULL + "888.777.666" + Constants.NULL + nfv.getIndexedFieldValue() + Constants.NULL
                                + nfv.getIndexedFieldName());
                // @formatter:off
                TermWeight.Info info = TermWeight.Info.newBuilder()
                        .addAllTermOffset(indexes.get(term))
                        .addScore(scores[i])
                        .addPrevSkips(0)
                        .setZeroOffsetMatch(false)
                        .build();
                // @formatter:on
                Key key = new Key(row, colf, colq, new ColumnVisibility("ALL"), new Date().getTime());
                Value value = new Value(info.toByteArray());
                Map.Entry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
                source.add(entry);
                i++;
            }
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
    public void tearDown() {
        options.clear();
    }

    private void givenOptions(String field, int start, int end) {
        options.put(FIELD_NAME, field);
        options.put(START_OFFSET, String.valueOf(start));
        options.put(END_OFFSET, String.valueOf(end));
    }

    private void initIterator() throws IOException {
        iterator.init(new SortedListKeyValueIterator(source), options, env);
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

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("BODY" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "quick brown fox jumped" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
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

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL
                        + "there is no greater divide in fandoms than that between star wars and star trek fans" + Constants.NULL + "XXXNOTSCOREDXXX"),
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

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "YOUR EXCERPT WAS BLANK! Maybe bad field or size?"
                        + Constants.NULL + "XXXNOTSCOREDXXX"), topKey.getColumnQualifier());
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

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
        iterator.seek(range, Collections.emptyList(), false);

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "123.456.789"), topKey.getColumnFamily());
        assertEquals(new Text("BAD_FIELD" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "YOUR EXCERPT WAS BLANK! Maybe bad field or size?"
                        + Constants.NULL + "XXXNOTSCOREDXXX"), topKey.getColumnQualifier());
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

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
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

    @Test
    public void testMatchFoundWithRemovedStoplistWord() throws IOException {
        givenOptions("BODY", 1, 5);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
        iterator.setTrimExcerpt(true);
        iterator.setOrigHalfSize(20);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("BODY" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "coldest tale ever" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testMatchFoundWithStoplistWordAndOutOfBoundsRange() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
        iterator.setTrimExcerpt(true);
        iterator.setOrigHalfSize(20);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL
                        + "somewhere far along the street they lost their soul to a person so mean" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testBracketsAroundSingleHit() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("street")));
        iterator.setTrimExcerpt(true);
        iterator.setOrigHalfSize(20);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL
                        + "somewhere far along the [street] they lost their soul to a person so mean" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testBracketsAroundMultipleHit() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("street", "person", "the", "the street")));
        iterator.setTrimExcerpt(true);
        iterator.setOrigHalfSize(20);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL
                        + "somewhere far along [the street] they lost their soul to a [person] so mean" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testDirectionBefore() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("street", "person", "the", "the street")));
        iterator.setDirection("BEFORE");
        iterator.setTrimExcerpt(true);
        iterator.setOrigHalfSize(20);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL
                        + "somewhere far along [the street] they lost their soul to a [person]" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testDirectionAfter() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("street", "person", "the", "the street")));
        iterator.setDirection("AFTER");
        iterator.setTrimExcerpt(true);
        iterator.setOrigHalfSize(20);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "[the street] they lost their soul to a [person] so mean"
                        + Constants.NULL + "XXXNOTSCOREDXXX"), topKey.getColumnQualifier());
    }

    @Test
    public void testTrimBefore() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("street")));
        iterator.setDirection("BEFORE");
        iterator.setOrigHalfSize(1);
        iterator.setTrimExcerpt(true);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "along the [street]" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testTrimAfter() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("street")));
        iterator.setDirection("AFTER");
        iterator.setOrigHalfSize(1);
        iterator.setTrimExcerpt(true);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "[street] they lost" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testTrimBoth() throws IOException {
        givenOptions("CONTENT", -10, 21);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("street")));
        iterator.setDirection("BOTH");
        iterator.setOrigHalfSize(1);
        iterator.setTrimExcerpt(true);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text("CONTENT" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + "the [street] they" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void testQuickFailStopWordFound() throws IOException {
        givenOptions("BODY", 1, 5);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "111.222.333"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("")));
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "111.222.333"), topKey.getColumnFamily());
        assertEquals(new Text(
                        "BODY" + Constants.NULL + "XXXWESKIPPEDAWORDXXX" + Constants.NULL + "XXXWESKIPPEDAWORDXXX" + Constants.NULL + "XXXWESKIPPEDAWORDXXX"),
                        topKey.getColumnQualifier());
        String[] parts = topKey.getColumnQualifier().toString().split(Constants.NULL);
        assertEquals(4, parts.length);
    }

    @Test
    public void testBracketsAroundMultipleHitMultiplePhrases() throws IOException {
        givenOptions("BODY", -10, 37);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "333.222.111"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("like to repeat", "ask questions", "stuff")));
        iterator.setTrimExcerpt(true);
        iterator.setOrigHalfSize(20);
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "333.222.111"), topKey.getColumnFamily());
        assertEquals(new Text("BODY" + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL
                        + "we [like to repeat] [stuff] do not [ask questions] we [like to repeat] [stuff]" + Constants.NULL + "XXXNOTSCOREDXXX"),
                        topKey.getColumnQualifier());
    }

    @Test
    public void multiWordAtOneOffsetWithScores() throws IOException {
        givenOptions("BODY", 0, 11);
        initIterator();

        Key startKey = new Key(row, new Text("email" + Constants.NULL + "888.777.666"));
        Range range = new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);

        iterator.setHitTermsList(new ArrayList<>(List.of("four", "cinco")));
        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(new Text("email" + Constants.NULL + "888.777.666"), topKey.getColumnFamily());
        assertEquals(new Text("BODY" + Constants.NULL + "one(85) dos(88) three(81) [four(89)] [cinco(88)] seis(88) siete(88) ocho(98) nine(86) diez(98)"
                        + Constants.NULL + "one dos three [four(89)] [cinco(88)] seis siete ocho nine diez" + Constants.NULL
                        + "one dos three [four] five seis siete ocho nine diez"), topKey.getColumnQualifier());
    }
}
