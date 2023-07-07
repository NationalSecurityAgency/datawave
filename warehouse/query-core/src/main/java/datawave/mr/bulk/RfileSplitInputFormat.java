package datawave.mr.bulk;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.RFileInputFormat;
import datawave.mr.bulk.split.FileRangeSplit;
import datawave.mr.bulk.split.TabletSplitSplit;

public class RfileSplitInputFormat extends MultiRfileInputformat {

    @Override
    public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RangeRecordReader();
    }

    /**
     * Return the lists of computed slit points
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        AccumuloHelper cbHelper = new AccumuloHelper();
        cbHelper.setup(job.getConfiguration());

        String tableName = BulkInputFormat.getTablename(job.getConfiguration());
        boolean autoAdjust = BulkInputFormat.getAutoAdjustRanges(job.getConfiguration());
        List<Range> ranges = autoAdjust ? Range.mergeOverlapping(BulkInputFormat.getRanges(job.getConfiguration()))
                        : BulkInputFormat.getRanges(job.getConfiguration());

        if (ranges.isEmpty()) {
            ranges = Lists.newArrayListWithCapacity(1);
            ranges.add(new Range());
        }

        Path[] paths = RFileInputFormat.getInputPaths(job);
        List<InputSplit> inputSplits = Lists.newArrayList();

        Configuration conf = job.getConfiguration();

        String[] hosts = {"ingest"};
        for (Path path : paths) {
            TabletSplitSplit split = new TabletSplitSplit(1);
            long length = path.getFileSystem(conf).getFileStatus(path).getLen();
            FileRangeSplit frSplit = new FileRangeSplit(ranges, path, 0, length, hosts);
            try {
                split.add(frSplit);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            inputSplits.add(split);
        }

        return inputSplits;
    }
}
