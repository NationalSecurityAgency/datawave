package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.reindex.ShardReindexJob.FI_END;
import static datawave.ingest.mapreduce.job.reindex.ShardReindexJob.FI_START;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
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
            Assert.assertEquals(expectedRange, rangeIterator.next());
        }

        Assert.assertFalse(rangeIterator.hasNext());
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
}
