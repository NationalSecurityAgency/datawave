package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.ContentSummaryIterator.ONLY_SPECIFIED;
import static datawave.query.iterator.logic.ContentSummaryIterator.SUMMARY_SIZE;
import static datawave.query.iterator.logic.ContentSummaryIterator.VIEW_NAMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import datawave.query.Constants;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.table.constants.ColumnFamilyConstants;

@ExtendWith(EasyMockExtension.class)
public class ContentSummaryIteratorTest extends EasyMockSupport {

    private static final Text row = new Text("20220115_1");
    private static final Text colf = new Text(ColumnFamilyConstants.FULL_CONTENT);

    @Mock
    private IteratorEnvironment env;
    private static final List<Map.Entry<Key,Value>> source = new ArrayList<>();
    private final Map<String,String> options = new HashMap<>();
    private final ContentSummaryIterator iterator = new ContentSummaryIterator();

    @BeforeAll
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

    @AfterEach
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
    public void testWildcardSummariesRespectTheRequestedSize() throws IOException {
        givenOptions("CONTENT3*", 4, true);
        initIterator();

        Key startKey = new Key(row, new Text("pdf" + Constants.NULL + "111.222.333"));
        iterator.seek(new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false), Collections.emptyList(), false);

        assertTrue(iterator.hasTop());
        assertEquals("CONTENT31: test\nCONTENT32: test", iterator.getTopKey().getColumnQualifier().toString());
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

    @Test
    public void testOnlyReadsHighestPriorityViewWhenItSortsFirst() throws IOException {
        assertSelectedViews("A,B,Z", List.of("A", "B", "Z"), List.of("A"), List.of("A"));
    }

    @Test
    public void testLaterHigherPriorityViewsReplaceEarlierCandidates() throws IOException {
        assertSelectedViews("Z,B,A", List.of("A", "B", "Z"), List.of("Z"), List.of("A", "B", "Z"));
    }

    @Test
    public void testBestFallbackReplacesEarlierCandidateWhenHigherPriorityIsMissing() throws IOException {
        assertSelectedViews("MISSING,B,A", List.of("A", "B", "Z"), List.of("B"), List.of("A", "B"));
    }

    @Test
    public void testHighestPriorityWildcardReadsAllItsViews() throws IOException {
        assertSelectedViews("B*,A,Z", List.of("A", "B1", "B2", "Z"), List.of("B1", "B2"), List.of("A", "B1", "B2"));
    }

    @Test
    public void testFallbackWildcardReadsAllItsViews() throws IOException {
        assertSelectedViews("MISSING,B*,A,Z", List.of("A", "B1", "B2", "Z"), List.of("B1", "B2"), List.of("A", "B1", "B2"));
    }

    @Test
    public void testHigherPriorityExactViewExcludesLowerPriorityWildcardViews() throws IOException {
        assertSelectedViews("B,B*", List.of("B", "B1", "B2"), List.of("B"), List.of("B"));
    }

    @Test
    public void testHigherPriorityWildcardIncludesLowerPriorityExactView() throws IOException {
        assertSelectedViews("*,B", List.of("A", "B", "Z"), List.of("A", "B", "Z"), List.of("A", "B", "Z"));
    }

    @Test
    public void testHigherPriorityWildcardExcludesOtherWildcardMatches() throws IOException {
        assertSelectedViews("B*,*", List.of("A", "B1", "B2", "Z"), List.of("B1", "B2"), List.of("A", "B1", "B2"));
    }

    @Test
    public void testHigherPriorityWildcardReplacesAllEarlierWildcardPrefixes() throws IOException {
        assertSelectedViews("Z*,A*", List.of("A1", "A2", "Z1", "Z2"), List.of("Z1", "Z2"), List.of("A1", "A2", "Z1", "Z2"));
    }

    @Test
    public void testNoMatchingViewReadsNoValues() throws IOException {
        assertSelectedViews("MISSING", List.of("A", "B"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testSelectionUsesSummaryCreatorsCaseSensitiveMatching() throws IOException {
        assertSelectedViews("a,B", List.of("A", "B"), List.of("B"), List.of("B"));
    }

    @Test
    public void testPrioritySelectionResetsBetweenDocumentsAndPreservesRangeEnd() throws IOException {
        TrackingSource contentSource = new TrackingSource(List.of(contentEntry("first", "B"), contentEntry("first", "Z"), contentEntry("second", "A"),
                        contentEntry("second", "B"), contentEntry("third", "A")));
        givenOptions("A,B", 100, true);
        iterator.init(contentSource, options, env);
        Key startKey = new Key(row, new Text("email" + Constants.NULL + "first"));
        Key endKey = new Key(row, new Text("email" + Constants.NULL + "second"));
        iterator.seek(new Range(startKey, true, endKey.followingKey(PartialKey.ROW_COLFAM), false), Collections.emptyList(), false);

        assertTrue(iterator.hasTop());
        assertEquals("email" + Constants.NULL + "first", iterator.getTopKey().getColumnFamily().toString());
        assertEquals("B: B content", iterator.getTopKey().getColumnQualifier().toString());

        iterator.next();
        assertTrue(iterator.hasTop());
        assertEquals("email" + Constants.NULL + "second", iterator.getTopKey().getColumnFamily().toString());
        assertEquals("A: A content", iterator.getTopKey().getColumnQualifier().toString());

        iterator.next();
        assertFalse(iterator.hasTop());
        assertEquals(List.of("B", "A"), contentSource.viewsRead);
        assertEquals(1, contentSource.seekCount);
        assertEquals(List.of("email" + Constants.NULL + "first" + Constants.NULL + "B", "email" + Constants.NULL + "first" + Constants.NULL + "Z",
                        "email" + Constants.NULL + "second" + Constants.NULL + "A", "email" + Constants.NULL + "second" + Constants.NULL + "B"),
                        contentSource.keysAdvanced);
    }

    @Test
    public void testBoundedSummaryOfSmallValueWithLargeExpansion() throws IOException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        byte[] block = new byte[8192];
        Arrays.fill(block, (byte) 'x');
        try (GZIPOutputStream gzip = new GZIPOutputStream(Base64.getEncoder().wrap(encoded))) {
            for (int i = 0; i < 65536; i++) {
                gzip.write(block);
            }
        }
        assertSummary(encoded.toByteArray(), 9000, "x".repeat(1500));
    }

    @Test
    public void testLargeValueCanExpandToLessThanMaxSummarySize() throws IOException {
        // MIME Base64 ignores whitespace. Exercise a large stored value with a short expanded document.
        byte[] encoded = encodeContent("short \u00e9\u4e2d\ud83d\ude00");
        byte[] padded = Arrays.copyOf(encoded, 100 * 1024 * 1024);
        Arrays.fill(padded, encoded.length, padded.length, (byte) '\n');
        assertSummary(padded, 1500, "short \u00e9\u4e2d\ud83d\ude00");
    }

    @Test
    public void testUtf8SummaryDoesNotSplitSurrogatePairs() throws IOException {
        assertSummary(encodeContent("a\u00e9\u4e2d\ud83d\ude00tail"), 4, "a\u00e9\u4e2d");
    }

    @Test
    public void testGzipSummaryWhenBase64ErrorIsBeyondThePrefix() throws IOException {
        byte[] content = new byte[64 * 1024];
        new Random(0).nextBytes(content);
        Arrays.fill(content, 0, 1500, (byte) 'x');
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(Base64.getEncoder().wrap(encoded))) {
            gzip.write(content);
        }
        byte[] withInvalidSuffix = encoded.toByteArray();
        withInvalidSuffix[withInvalidSuffix.length - 16] = '=';
        assertSummary(withInvalidSuffix, 1500, "x".repeat(1500));
    }

    @Test
    public void testRawSummaryWhenInvalidBase64EndingIsBeyondThePrefix() throws IOException {
        String raw = "hello ".repeat(4000) + "A";
        assertSummary(raw.getBytes(StandardCharsets.UTF_8), 1500, raw.substring(0, 1500));
    }

    @Test
    public void testRawSummaryWhenBase64AlphabetFollowsPadding() throws IOException {
        String raw = "YWJjZA== hello";
        assertSummary(raw.getBytes(StandardCharsets.UTF_8), 1500, raw);
    }

    private void assertSelectedViews(String viewOrder, List<String> availableViews, List<String> expectedSummaryViews, List<String> expectedViewsRead)
                    throws IOException {
        List<Map.Entry<Key,Value>> entries = new ArrayList<>();
        for (String view : availableViews) {
            entries.add(contentEntry("priority", view));
        }
        TrackingSource contentSource = new TrackingSource(entries);
        givenOptions(viewOrder, 100, true);
        iterator.init(contentSource, options, env);
        Key startKey = new Key(row, new Text("email" + Constants.NULL + "priority"));
        iterator.seek(new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false), Collections.emptyList(), false);

        if (expectedSummaryViews.isEmpty()) {
            assertFalse(iterator.hasTop());
        } else {
            assertTrue(iterator.hasTop());
            String[] expectedSummaries = expectedSummaryViews.stream().map(view -> view + ": " + view + " content").toArray(String[]::new);
            assertEquals(Set.of(expectedSummaries), Set.of(iterator.getTopKey().getColumnQualifier().toString().split("\n")));
            assertEquals(new Text("ALL"), iterator.getTopKey().getColumnVisibility());
            iterator.next();
            assertFalse(iterator.hasTop());
        }
        assertEquals(expectedViewsRead, contentSource.viewsRead);
        assertEquals(1, contentSource.seekCount);
        assertEquals(entries.stream().sorted(Map.Entry.comparingByKey()).map(entry -> entry.getKey().getColumnQualifier().toString())
                        .collect(Collectors.toList()), contentSource.keysAdvanced);
    }

    private static Map.Entry<Key,Value> contentEntry(String uid, String view) throws IOException {
        Key key = new Key(row, colf, new Text("email" + Constants.NULL + uid + Constants.NULL + view), new ColumnVisibility("ALL"), 1L);
        return new AbstractMap.SimpleEntry<>(key, new Value(encodeContent(view + " content")));
    }

    private static class TrackingSource extends WrappingIterator {
        private final List<String> viewsRead = new ArrayList<>();
        private final List<String> keysAdvanced = new ArrayList<>();
        private int seekCount;

        private TrackingSource(List<Map.Entry<Key,Value>> entries) {
            setSource(new SortedListKeyValueIterator(entries));
        }

        @Override
        public Value getTopValue() {
            String qualifier = getTopKey().getColumnQualifier().toString();
            viewsRead.add(qualifier.substring(qualifier.lastIndexOf(Constants.NULL) + 1));
            return super.getTopValue();
        }

        @Override
        public void next() throws IOException {
            keysAdvanced.add(getTopKey().getColumnQualifier().toString());
            super.next();
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            seekCount++;
            super.seek(range, columnFamilies, inclusive);
        }
    }

    private void assertSummary(byte[] encodedContent, int size, String expected) throws IOException {
        String dtUid = "email" + Constants.NULL + "bounded";
        Key contentKey = new Key(row, colf, new Text(dtUid + Constants.NULL + "CONTENT"), new ColumnVisibility("ALL"), 1L);
        List<Map.Entry<Key,Value>> contentSource = List.of(new AbstractMap.SimpleEntry<>(contentKey, new Value(encodedContent, false)));
        givenOptions("CONTENT", size, true);
        iterator.init(new SortedListKeyValueIterator(contentSource), options, env);
        Key startKey = new Key(row, new Text(dtUid));
        iterator.seek(new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false), Collections.emptyList(), false);
        assertTrue(iterator.hasTop());
        assertEquals("CONTENT: " + expected, iterator.getTopKey().getColumnQualifier().toString());
        assertEquals(new Text("ALL"), iterator.getTopKey().getColumnVisibility());
    }

    private static byte[] encodeContent(String content) throws IOException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(Base64.getEncoder().wrap(encoded))) {
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return encoded.toByteArray();
    }
}
