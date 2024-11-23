package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.reindex.ShardReindexJob.FI_END;
import static datawave.ingest.mapreduce.job.reindex.ShardReindexJob.FI_START;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.mapreduce.job.util.AccumuloUtil;
import datawave.ingest.mapreduce.job.util.RFileUtil;

public class ShardReindexJobTest extends EasyMockSupport {

    @Before
    public void setup() {}

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

    @Test
    public void buildSplittableRanges_singleSplitTest() throws Throwable {
        AccumuloUtil accumuloUtil = createMock(AccumuloUtil.class);
        RFileUtil rFileUtil = createMock(RFileUtil.class);

        List<Map.Entry<String,List<String>>> results = new ArrayList<>();
        results.add(new AbstractMap.SimpleImmutableEntry<>("20241121", Collections.singletonList("/some/path/to/an/rfile")));
        Range splitRange = new Range();

        expect(accumuloUtil.getFilesFromMetadataBySplit("myShardTable", "20241121", "20241122")).andReturn(results);
        expect(rFileUtil.getRangeSplits(Collections.singletonList("/some/path/to/an/rfile"), new Key("20241121"),
                        new Key("20241121", "" + '\uFFFF', "" + '\uFFFF'), -1, Function.identity())).andReturn(Collections.singletonList(splitRange));

        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, 1, -1, ShardReindexMapper.BatchMode.NONE, "myShardTable",
                        "20241121", "20241122");

        verifyAll();

        assertTrue(ranges.size() == 1);
    }

    @Test
    public void applyExclusions_noExclusionsTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + "\u0000"), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), Collections.emptyList());

        assertTrue(ranges.size() == 1);
        assertTrue(ranges.get(0) == orig);
    }

    @Test
    public void applyExclusions_singleRangeSplitTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + "\u0000"), false);
        Range exclude = new Range(new Key("20241122", "a" + '\u0000'), true, new Key("20241122", "a" + '\u0001'), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), Collections.singletonList(exclude));

        assertTrue(ranges.size() == 2);

        // verify the exclusion was totally removed
        for (Range range: ranges) {
            assertNull(range.clip(exclude, true));
        }

        verifyExclude(orig, ranges.get(0), ranges.get(1), exclude);

        // verify the begin and end remain the same
        assertTrue(orig.getStartKey().equals(ranges.get(0).getStartKey()));
        assertEquals(orig.isStartKeyInclusive(), ranges.get(0).isStartKeyInclusive());
        assertTrue(orig.getEndKey().equals(ranges.get(1).getEndKey()));
        assertEquals(orig.isEndKeyInclusive(), ranges.get(1).isEndKeyInclusive());
    }

    @Test
    public void applyExclusions_doubleRangeSplitTest() {
        Range orig = new Range(new Key("20241122"), true, new Key("20241122" + "\u0000"), false);
        Range exclude1 = new Range(new Key("20241122", "a" + '\u0000'), true, new Key("20241122", "a" + '\u0001'), false);
        Range exclude2 = new Range(new Key("20241122", "c" + '\u0000'), true, new Key("20241122", "c" + '\u0001'), false);
        List<Range> ranges = ShardReindexJob.applyExclusions(Collections.singletonList(orig), List.of(exclude1, exclude2));

        assertTrue( ranges.size() == 3);

        // verify the exclusion was totally removed
        for (Range range: ranges) {
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

    private void verifyExclude(Range orig, Range new1, Range new2, Range exclude) {
        // verify the hole
        assertFalse(new1.isEndKeyInclusive());
        assertTrue(new1.getEndKey().equals(exclude.getStartKey()));

        assertTrue(new2.isStartKeyInclusive());
        assertTrue(new2.getStartKey().equals(exclude.getEndKey()));
    }
}
