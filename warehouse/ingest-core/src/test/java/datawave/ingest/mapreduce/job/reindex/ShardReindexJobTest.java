package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.reindex.ShardReindexJob.FI_END;
import static datawave.ingest.mapreduce.job.reindex.ShardReindexJob.FI_START;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.mapreduce.job.util.AccumuloUtil;
import datawave.ingest.mapreduce.job.util.RFileUtil;

public class ShardReindexJobTest extends EasyMockSupport {

    private AccumuloUtil accumuloUtil;
    private RFileUtil rFileUtil;

    @Before
    public void setup() {
        accumuloUtil = createMock(AccumuloUtil.class);
        rFileUtil = createMock(RFileUtil.class);
    }

    private Collection<Range> buildRanges(String row, int shards) {
        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < shards; i++) {
            Text shardRow = new Text(row + "_" + i);
            ranges.add(new Range(new Key(shardRow, FI_START), true, new Key(shardRow, FI_END), true));
        }

        return ranges;
    }

    private void verifyRanges(Collection<Range> ranges, Collection<Range> expected) {
        Iterator<Range> rangeIterator = ranges.iterator();
        for (Range expectedRange : expected) {
            assertTrue(rangeIterator.hasNext());
            assertEquals(expectedRange, rangeIterator.next());
        }

        assertFalse(rangeIterator.hasNext());
    }

    @Test
    public void oneDayRange_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildFiRanges("20230925", "20230925", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230925", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void twoDayRange_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildFiRanges("20230925", "20230926", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230925", 5));
        expected.addAll(buildRanges("20230926", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void oneWeekRange_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildFiRanges("20230901", "20230907", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230901", 5));
        expected.addAll(buildRanges("20230902", 5));
        expected.addAll(buildRanges("20230903", 5));
        expected.addAll(buildRanges("20230904", 5));
        expected.addAll(buildRanges("20230905", 5));
        expected.addAll(buildRanges("20230906", 5));
        expected.addAll(buildRanges("20230907", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void monthRollover_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildFiRanges("20230831", "20230901", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230831", 5));
        expected.addAll(buildRanges("20230901", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void singleSplit_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildFiRanges("20230831", "20230831", 1);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230831", 1));

        verifyRanges(ranges, expected);
    }

    @Test
    public void noInputFiles_noDates_test() {
        // TODO
    }

    @Test(expected = AccumuloException.class)
    public void buildSplittableRanges_noTableTest() throws Throwable {
        AccumuloUtil accumuloUtil = createMock(AccumuloUtil.class);
        RFileUtil rFileUtil = createMock(RFileUtil.class);

        expect(accumuloUtil.getFilesFromMetadataBySplit("myShardTable", "20241121", "20241122"))
                        .andThrow(new RuntimeException("some error", new AccumuloException("table not found")));

        replayAll();

        try {
            Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                            "20241121", "20241122");
        } catch (RuntimeException e) {
            throw e.getCause();
        }

        verifyAll();
    }

    @Test
    public void buildSplittableRanges_noFilesTest() throws Throwable {
        AccumuloUtil accumuloUtil = createMock(AccumuloUtil.class);
        RFileUtil rFileUtil = createMock(RFileUtil.class);

        expect(accumuloUtil.getFilesFromMetadataBySplit("myShardTable", "20241121", "20241122")).andReturn(Collections.emptyList());

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122");

        verifyAll();

        assertTrue(ranges.size() == 0);
    }

    private void expectSplittableRangeSetup(Range result) throws AccumuloException, IOException {
        List<Map.Entry<String,List<String>>> results = new ArrayList<>();
        results.add(new AbstractMap.SimpleImmutableEntry<>("20241121", Collections.singletonList("/some/path/to/an/rfile")));

        expect(accumuloUtil.getFilesFromMetadataBySplit("myShardTable", "20241121", "20241122")).andReturn(results);
        expect(rFileUtil.getRangeSplits(Collections.singletonList("/some/path/to/an/rfile"), new Key("20241121"),
                        new Key("20241121", "" + '\uFFFF', "" + '\uFFFF'), -1, Function.identity())).andReturn(Collections.singletonList(result));
    }

    @Test
    public void buildSplittableRanges_singleSplitTest() throws Throwable {
        Range splitRange = new Range();
        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122");

        verifyAll();

        assertTrue(ranges.size() == 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void buildSplitRanges_sameStartEndTest() {
        replayAll();

        ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable", "20241121", "20241121");

        verifyAll();
    }

    @Test
    public void buildSplitRanges_excludeFiTest() throws AccumuloException, IOException {
        Range splitRange = new Range("20241121", true, "20241121" + '\u0000', false);
        Range expected1 = new Range(new Key("20241121"), true, new Key("20241121", "fi" + '\u0000'), false);
        Range expected2 = new Range(new Key("20241121", "fi" + '\u0001'), true, new Key("20241121" + '\u0000'), false);

        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122", true, false, false, Collections.emptyList(), Collections.emptyList());

        verifyAll();

        assertEquals(2, ranges.size());
        Iterator<Range> itr = ranges.iterator();
        assertEquals(expected1, itr.next());
        assertEquals(expected2, itr.next());
    }

    @Test
    public void buildSplitRanges_excludeTfTest() throws AccumuloException, IOException {
        Range splitRange = new Range("20241121", true, "20241121" + '\u0000', false);
        Range expected1 = new Range(new Key("20241121"), true, new Key("20241121", "tf"), false);
        Range expected2 = new Range(new Key("20241121", "tf" + '\u0000'), true, new Key("20241121" + '\u0000'), false);

        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122", false, true, false, Collections.emptyList(), Collections.emptyList());

        verifyAll();

        assertEquals(2, ranges.size());
        Iterator<Range> itr = ranges.iterator();
        assertEquals(expected1, itr.next());
        assertEquals(expected2, itr.next());
    }

    @Test
    public void buildSplitRanges_excludeDTest() throws AccumuloException, IOException {
        Range splitRange = new Range("20241121", true, "20241121" + '\u0000', false);
        Range expected1 = new Range(new Key("20241121"), true, new Key("20241121", "d"), false);
        Range expected2 = new Range(new Key("20241121", "d" + '\u0000'), true, new Key("20241121" + '\u0000'), false);

        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122", false, false, true, Collections.emptyList(), Collections.emptyList());

        verifyAll();

        assertEquals(2, ranges.size());
        Iterator<Range> itr = ranges.iterator();
        assertEquals(expected1, itr.next());
        assertEquals(expected2, itr.next());
    }

    @Test
    public void buildSplitRanges_excludeDataTypesTest() throws AccumuloException, IOException {
        Range splitRange = new Range("20241121", true, "20241121" + '\u0000', false);
        Range expected1 = new Range(new Key("20241121"), true, new Key("20241121", "sample" + '\u0000'), false);
        Range expected2 = new Range(new Key("20241121", "sample" + '\u0001'), true, new Key("20241121" + '\u0000'), false);

        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122", false, false, false, Collections.emptyList(), Collections.singletonList("sample"));

        verifyAll();

        assertEquals(2, ranges.size());
        Iterator<Range> itr = ranges.iterator();
        assertEquals(expected1, itr.next());
        assertEquals(expected2, itr.next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void buildSplitRanges_mixedDataTypesTest() throws AccumuloException, IOException {
        Range splitRange = new Range("20241121", true, "20241121" + '\u0000', false);
        Range expected1 = new Range(new Key("20241121"), true, new Key("20241121", "sample" + '\u0000'), false);
        Range expected2 = new Range(new Key("20241121", "sample" + '\u0001'), true, new Key("20241121" + '\u0000'), false);

        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122", false, false, false, Collections.singletonList("sample2"), Collections.singletonList("sample"));

        verifyAll();
    }

    @Test
    public void buildSplitRanges_includeDataTypesTest() throws AccumuloException, IOException {
        Range splitRange = new Range("20241121", true, "20241121" + '\u0000', false);
        Range expected1 = new Range(new Key("20241121", "sample" + '\u0000'), true, new Key("20241121", "sample" + '\u0001'), false);

        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122", true, true, true, Collections.singletonList("sample"), Collections.emptyList());

        verifyAll();

        // dataType only
        assertEquals(1, ranges.size());
        Iterator<Range> itr = ranges.iterator();
        assertEquals(expected1, itr.next());
    }

    @Test
    public void buildSplitRanges_includeDataTypesWithoutExcludesTest() throws AccumuloException, IOException {
        Range splitRange = new Range("20241121", true, "20241121" + '\u0000', false);
        Range expected1 = new Range(new Key("20241121", "d"), true, new Key("20241121", "d" + '\u0000'), false);
        Range expected2 = new Range(new Key("20241121", "fi" + '\u0000'), true, new Key("20241121", "fi" + '\u0001'), false);
        Range expected3 = new Range(new Key("20241121", "sample" + '\u0000'), true, new Key("20241121", "sample" + '\u0001'), false);
        Range expected4 = new Range(new Key("20241121", "tf"), true, new Key("20241121", "tf" + '\u0000'), false);

        expectSplittableRangeSetup(splitRange);

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122", false, false, false, Collections.singletonList("sample"), Collections.emptyList());

        verifyAll();

        // sort so the order is predictable
        List<Range> sorted = new ArrayList<>(ranges);
        Collections.sort(sorted);

        // fi, tf, d, dataType only
        assertEquals(4, sorted.size());
        Iterator<Range> itr = sorted.iterator();
        assertEquals(expected1, itr.next());
        assertEquals(expected2, itr.next());
        assertEquals(expected3, itr.next());
        assertEquals(expected4, itr.next());
    }

    @Test
    public void applyExclusions_noExclusionsTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + "\u0000"), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), Collections.emptyList());

        assertTrue(ranges.size() == 1);
        assertTrue(ranges.get(0) == orig);
    }

    // exclude out of the center of a range
    @Test
    public void applyExclusions_singleRangeSplitTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + "\u0000"), false);
        Range exclude = new Range(new Key("20241122", "a" + '\u0000'), true, new Key("20241122", "a" + '\u0001'), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), Collections.singletonList(exclude));

        assertTrue(ranges.size() == 2);

        // verify the exclusion was totally removed
        for (Range range : ranges) {
            assertNull(range.clip(exclude, true));
        }

        verifyExclude(orig, ranges.get(0), ranges.get(1), exclude);

        // verify the begin and end remain the same
        assertTrue(orig.getStartKey().equals(ranges.get(0).getStartKey()));
        assertEquals(orig.isStartKeyInclusive(), ranges.get(0).isStartKeyInclusive());
        assertTrue(orig.getEndKey().equals(ranges.get(1).getEndKey()));
        assertEquals(orig.isEndKeyInclusive(), ranges.get(1).isEndKeyInclusive());
    }

    // exclude an entire range
    @Test
    public void applyExclusions_wholeRangeTest() {
        Range orig = new Range(new Key("20241122", "ab"), true, new Key("20241122", "az"), false);
        Range exclude = new Range(new Key("20241122", "a"), true, new Key("20241122", "a" + '\uFFFF'), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), Collections.singletonList(exclude));

        assertTrue(ranges.isEmpty());
    }

    // exclude one range, no impact to the other
    @Test
    public void applyExclusions_wholeRangeMixedTest() {
        Range orig1 = new Range(new Key("20241122", "ab"), true, new Key("20241122", "az"), false);
        Range orig2 = new Range(new Key("20241122", "1"), true, new Key("20241122", "2"), false);
        Range exclude = new Range(new Key("20241122", "a"), true, new Key("20241122", "a" + '\uFFFF'), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(List.of(orig1, orig2), Collections.singletonList(exclude));

        assertFalse(ranges.isEmpty());
        assertEquals(1, ranges.size());
        assertEquals(orig2, ranges.get(0));
    }

    // test trim leading edge
    @Test
    public void applyExclusions_trimLeadingEdgeTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241123"), false);
        Range exclude1 = new Range(new Key("20241122"), true, new Key("20241122", "a" + '\uFFFF'), false);
        Range expected = new Range(new Key("20241122", "a" + '\uFFFF'), true, new Key("20241123"), false);

        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), List.of(exclude1));

        assertFalse(ranges.isEmpty());
        assertEquals(1, ranges.size());
        assertEquals(expected, ranges.get(0));
    }

    // test trim leading edge without inclusion, actually creates a split
    @Test
    public void applyExclusions_trimLeadingInclusiveHoleTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241123"), false);
        Range exclude1 = new Range(new Key("20241122"), false, new Key("20241122", "a" + '\uFFFF'), false);
        Range expected = new Range(new Key("20241122", "a" + '\uFFFF'), true, new Key("20241123"), false);
        Range expectedHole = new Range(new Key("20241122"), true, new Key("20241122"), true);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), List.of(exclude1));

        assertFalse(ranges.isEmpty());
        assertEquals(2, ranges.size());
        assertEquals(expectedHole, ranges.get(0));
        assertEquals(expected, ranges.get(1));
    }

    // test trim trailing edge
    @Test
    public void applyExclusions_trimTrailingEdgeTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241123"), false);
        Range exclude2 = new Range(new Key("20241122", "z"), true, new Key("20241123"), true);
        Range expected = new Range(new Key("20241122"), true, new Key("20241122", "z"), false);

        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), List.of(exclude2));

        assertFalse(ranges.isEmpty());
        assertEquals(1, ranges.size());
        assertEquals(expected, ranges.get(0));
    }

    // test trim trailing edge
    @Test
    public void applyExclusions_trimTrailingEdgeInclusiveHoleTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241123"), true);
        Range exclude2 = new Range(new Key("20241122", "z"), true, new Key("20241123"), false);

        Range expected = new Range(new Key("20241122"), true, new Key("20241122", "z"), false);
        Range expectedHole = new Range(new Key("20241123"), true, new Key("20241123"), true);

        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), List.of(exclude2));

        assertFalse(ranges.isEmpty());
        assertEquals(2, ranges.size());
        assertEquals(expected, ranges.get(0));
        assertEquals(expectedHole, ranges.get(1));
    }

    // trim both sides of a range
    @Test
    public void applyExclusions_trimEdgesTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241123"), false);
        Range exclude1 = new Range(new Key("20241122"), true, new Key("20241122", "a" + '\uFFFF'), false);
        Range exclude2 = new Range(new Key("20241122", "z"), true, new Key("20241123"), true);
        Range expected = new Range(new Key("20241122", "a" + '\uFFFF'), true, new Key("20241122", "z"), false);

        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), List.of(exclude1, exclude2));

        assertFalse(ranges.isEmpty());
        assertEquals(1, ranges.size());
        assertEquals(expected, ranges.get(0));
    }

    // slice the range multiple times
    @Test
    public void applyExclusions_doubleRangeSplitTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + "\u0000"), false);
        Range exclude1 = new Range(new Key("20241122", "a" + '\u0000'), true, new Key("20241122", "a" + '\u0001'), false);
        Range exclude2 = new Range(new Key("20241122", "c" + '\u0000'), true, new Key("20241122", "c" + '\u0001'), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), List.of(exclude1, exclude2));

        assertTrue(ranges.size() == 3);

        // verify the exclusion was totally removed
        for (Range range : ranges) {
            for (Range exclude : List.of(exclude1, exclude2)) {
                assertNull(range.clip(exclude, true));
            }
        }

        verifyExclude(orig, ranges.get(0), ranges.get(1), exclude1);
        verifyExclude(orig, ranges.get(1), ranges.get(2), exclude2);

        // verify the begin and end remain the same
        assertTrue(orig.getStartKey().equals(ranges.get(0).getStartKey()));
        assertEquals(orig.isStartKeyInclusive(), ranges.get(0).isStartKeyInclusive());
        assertTrue(orig.getEndKey().equals(ranges.get(2).getEndKey()));
        assertEquals(orig.isEndKeyInclusive(), ranges.get(2).isEndKeyInclusive());
    }

    @Test
    public void applyInclusions_missTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + '\u0000'), false);
        Range include = new Range(new Key("20241123"), true, new Key("20241123"), false);
        List<Range> ranges = ShardReindexJob.applyInclusions(Collections.singletonList(orig), Collections.singletonList(include));

        assertTrue(ranges.isEmpty());
    }

    @Test
    public void applyInclusions_multiMissBoundaryTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + '\u0000'), false);
        Range include1 = new Range(new Key("20241122" + "\u0000"), true, new Key("20241123"), false);
        Range include2 = new Range(new Key("20241121"), true, new Key("20241122"), false);
        List<Range> ranges = ShardReindexJob.applyInclusions(Collections.singletonList(orig), List.of(include1, include2));

        assertTrue(ranges.isEmpty());
    }

    @Test
    public void applyInclusions_partialTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + '\u0000'), false);
        Range include1 = new Range(new Key("20241122", "a"), true, new Key("20241123"), false);
        Range expected = new Range(new Key("20241122", "a"), true, new Key("20241122" + '\u0000'), false);
        List<Range> ranges = ShardReindexJob.applyInclusions(Collections.singletonList(orig), List.of(include1));

        assertFalse(ranges.isEmpty());
        assertEquals(expected, ranges.get(0));
    }

    private void verifyExclude(Range orig, Range new1, Range new2, Range exclude) {
        // verify the hole
        assertFalse(new1.isEndKeyInclusive());
        assertTrue(new1.getEndKey().equals(exclude.getStartKey()));

        assertTrue(new2.isStartKeyInclusive());
        assertTrue(new2.getStartKey().equals(exclude.getEndKey()));
    }
}
