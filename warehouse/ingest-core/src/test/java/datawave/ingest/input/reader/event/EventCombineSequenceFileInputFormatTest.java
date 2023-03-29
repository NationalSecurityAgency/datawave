package datawave.ingest.input.reader.event;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import datawave.data.hash.UID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.FakeIngestHelper;
import datawave.ingest.mapreduce.MapReduceTestUtil;
import datawave.ingest.mapreduce.SimpleDataTypeHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventCombineSequenceFileInputFormatTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(EventCombineSequenceFileInputFormatTest.class);
    private static Configuration conf = new Configuration();
    private static FileSystem localFs = null;

    static {
        try {
            conf.set("fs.defaultFS", "file:///");
            localFs = FileSystem.getLocal(conf);
        } catch (IOException e) {
            throw new RuntimeException("init failure", e);
        }
    }

    private static Path workDir =
            new Path(new Path(System.getProperty("test.build.data", "."), "data"),
                    "EventCombineSequenceFileInputFormatTest");

    @Test(timeout=10000)
    public void testFormat() throws IOException, InterruptedException {
        Job job = Job.getInstance(conf);

        Random random = new Random();
        long seed = random.nextLong();
        random.setSeed(seed);

        localFs.delete(workDir, true);
        FileInputFormat.setInputPaths(job, workDir);

        final int length = 10000;
        final int numFiles = 10;

        // create files with a variety of lengths
        createFiles(length, numFiles, random, job);

        TaskAttemptContext context = MapReduceTestUtil.createDummyMapTaskAttemptContext(job.getConfiguration());
        // create a combine split for the files
        InputFormat<IntWritable, RawRecordContainer> format =
                new EventCombineSequenceFileInputFormat<IntWritable>();
        for (int i = 0; i < 3; i++) {
            int numSplits =
                    random.nextInt(length/(SequenceFile.SYNC_INTERVAL/20)) + 1;
            LOG.info("splitting: requesting = " + numSplits);
            List<InputSplit> splits = format.getSplits(job);
            LOG.info("splitting: got =        " + splits.size());

            // we should have a single split as the length is comfortably smaller than
            // the block size
            assertEquals("We got more than one splits!", 1, splits.size());
            InputSplit split = splits.get(0);
            assertEquals("It should be CombineFileSplit",
                    CombineFileSplit.class, split.getClass());

            // check the split
            BitSet bits = new BitSet(length);
            RecordReader<IntWritable,RawRecordContainer> reader =
                    format.createRecordReader(split, context);
            MapContext<IntWritable,RawRecordContainer,IntWritable,RawRecordContainer> mcontext =
                    new MapContextImpl<>(job.getConfiguration(),
                            context.getTaskAttemptID(), reader, null, null,
                            MapReduceTestUtil.createDummyReporter(), split);
            reader.initialize(split, mcontext);
            assertEquals("reader class is CombineFileRecordReader.",
                    CombineFileRecordReader.class, reader.getClass());

            try {
                while (reader.nextKeyValue()) {
                    IntWritable key = reader.getCurrentKey();
                    RawRecordContainer value = reader.getCurrentValue();
                    assertNotNull("Value should not be null.", value);
                    final int k = key.get();
                    LOG.debug("read " + k);
                    assertFalse("Key in multiple partitions.", bits.get(k));
                    bits.set(k);
                }
            } finally {
                reader.close();
            }
            assertEquals("Some keys in no partition.", length, bits.cardinality());
        }
    }


    private static class Range {
        private final int start;
        private final int end;

        Range(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "(" + start + ", " + end + ")";
        }
    }

    private static Range[] createRanges(int length, int numFiles, Random random) {
        // generate a number of files with various lengths
        Range[] ranges = new Range[numFiles];
        for (int i = 0; i < numFiles; i++) {
            int start = i == 0 ? 0 : ranges[i-1].end;
            int end = i == numFiles - 1 ?
                    length :
                    (length/numFiles)*(2*i + 1)/2 + random.nextInt(length/numFiles) + 1;
            ranges[i] = new Range(start, end);
        }
        return ranges;
    }

    private static RawRecordContainerImpl getEvent(Job job) {
        RawRecordContainerImpl myEvent = new RawRecordContainerImpl();
        myEvent.addSecurityMarking("columnVisibility", "PRIVATE");
        myEvent.setId(UID.builder().newId());
        myEvent.setConf(job.getConfiguration());

        Instant i = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2016-04-26T01:31:53Z"));
        myEvent.setDate(i.getEpochSecond());
        return myEvent;
    }

    private static void createFiles(int length, int numFiles, Random random,
                                    Job job) throws IOException {
        Range[] ranges = createRanges(length, numFiles, random);

        for (int i = 0; i < numFiles; i++) {
            Path file = new Path(workDir, "test_" + i + ".seq");
            // create a file with length entries
            @SuppressWarnings("deprecation")
            SequenceFile.Writer writer =
                    SequenceFile.createWriter(localFs, job.getConfiguration(), file,
                            IntWritable.class, RawRecordContainerImpl.class);
            Range range = ranges[i];
            try {
                for (int j = range.start; j < range.end; j++) {
                    IntWritable key = new IntWritable(j);
                    byte[] data = new byte[random.nextInt(10)];
                    random.nextBytes(data);
                    RawRecordContainerImpl value = getEvent(job);
                    writer.append(key, value);
                }
            } finally {
                writer.close();
            }
        }
    }
}
