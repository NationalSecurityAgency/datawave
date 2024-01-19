package datawave.ingest.input.reader.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.hash.UID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;

public class EventCombineSequenceFileInputFormatTest {
    private static final Logger LOG = LoggerFactory.getLogger(EventCombineSequenceFileInputFormatTest.class);
    private Configuration conf;
    private FileSystem localFs;
    private Path workDir;
    private Job job;

    @Before
    public void setup() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        localFs = FileSystem.getLocal(conf);
        workDir = new Path(new Path(System.getProperty("test.build.data", "."), "data"), "EventCombineSequenceFileInputFormatTest");
        job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, workDir);
    }

    @After
    public void teardown() throws Exception {
        localFs.delete(workDir, true);
        localFs.close();
    }

    @Test
    public void testFormatCombinesMultipleFiles() throws Exception {
        createFile("File1.seq", 1);
        createFile("File2.seq", 1);
        TaskAttemptContext context = createDummyMapTaskAttemptContext(conf);
        // create a combine split for the files
        InputFormat<IntWritable,RawRecordContainer> format = new EventCombineSequenceFileInputFormat<IntWritable>();
        List<InputSplit> splits = format.getSplits(job);
        assertEquals("We got more than one splits!", 1, splits.size());
        InputSplit split = splits.get(0);
        RecordReader<IntWritable,RawRecordContainer> reader = format.createRecordReader(split, context);
        MapContext<IntWritable,RawRecordContainer,IntWritable,RawRecordContainer> mcontext = new MapContextImpl<>(job.getConfiguration(),
                        context.getTaskAttemptID(), reader, null, null, createDummyReporter(), split);
        reader.initialize(split, mcontext);
        Assert.assertEquals("File File1: Test Data 1", reader.getCurrentValue());
    }

    @Ignore
    @Test(timeout = 10000)
    public void testFormat() throws IOException, InterruptedException {

        Random random = new Random();
        long seed = random.nextLong();
        random.setSeed(seed);

        final int length = 10000;
        final int numFiles = 10;

        // create files with a variety of lengths
        createRandomFiles(length, numFiles, random);

        TaskAttemptContext context = createDummyMapTaskAttemptContext(conf);
        // create a combine split for the files
        InputFormat<IntWritable,RawRecordContainer> format = new EventCombineSequenceFileInputFormat<IntWritable>();
        for (int i = 0; i < 3; i++) {
            int numSplits = random.nextInt(length / (SequenceFile.SYNC_INTERVAL / 20)) + 1;
            LOG.info("splitting: requesting = " + numSplits);
            List<InputSplit> splits = format.getSplits(job);
            LOG.info("splitting: got = " + splits.size());

            // we should have a single split as the length is comfortably smaller than
            // the block size
            assertEquals("We got more than one splits!", 1, splits.size());
            InputSplit split = splits.get(0);
            assertEquals("It should be CombineFileSplit", CombineFileSplit.class, split.getClass());

            // check the split
            BitSet bits = new BitSet(length);
            RecordReader<IntWritable,RawRecordContainer> reader = format.createRecordReader(split, context);
            MapContext<IntWritable,RawRecordContainer,IntWritable,RawRecordContainer> mcontext = new MapContextImpl<>(job.getConfiguration(),
                            context.getTaskAttemptID(), reader, null, null, createDummyReporter(), split);
            reader.initialize(split, mcontext);
            assertEquals("reader class is CombineFileRecordReader.", CombineFileRecordReader.class, reader.getClass());

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

    public TaskAttemptContext createDummyMapTaskAttemptContext(Configuration conf) {
        TaskAttemptID tid = new TaskAttemptID("jt", 1, TaskType.MAP, 0, 0);
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, tid.toString());
        return new TaskAttemptContextImpl(conf, tid);
    }

    public StatusReporter createDummyReporter() {
        return new StatusReporter() {
            public void setStatus(String s) {}

            public void progress() {}

            @Override
            public float getProgress() {
                return 0;
            }

            public Counter getCounter(Enum<?> name) {
                return new Counters().findCounter(name);
            }

            public Counter getCounter(String group, String name) {
                return new Counters().findCounter(group, name);
            }
        };
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

    private Range[] createRanges(int length, int numFiles, Random random) {
        // generate a number of files with various lengths
        Range[] ranges = new Range[numFiles];
        for (int i = 0; i < numFiles; i++) {
            int start = i == 0 ? 0 : ranges[i - 1].end;
            int end = i == numFiles - 1 ? length : (length / numFiles) * (2 * i + 1) / 2 + random.nextInt(length / numFiles) + 1;
            ranges[i] = new Range(start, end);
        }
        return ranges;
    }

    private RawRecordContainerImpl getEvent(byte[] data) {
        RawRecordContainerImpl myEvent = new RawRecordContainerImpl();
        myEvent.addSecurityMarking("columnVisibility", "PRIVATE");
        myEvent.setId(UID.builder().newId());
        myEvent.setConf(conf);

        Instant i = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2016-04-26T01:31:53Z"));
        myEvent.setDate(i.getEpochSecond());
        // myEvent.setRawData(data);
        return myEvent;
    }

    private void createFile(String filename, int numEvents) throws IOException {
        Path file = new Path(workDir, filename);
        // create a file with length entries
        // @SuppressWarnings("deprecation")
        try (SequenceFile.Writer writer = SequenceFile.createWriter(localFs, conf, file, IntWritable.class, RawRecordContainerImpl.class)) {
            for (int i = 0; i < numEvents; i++) {
                IntWritable key = new IntWritable(i);
                byte[] data = String.format("File {}: Test Data {}", filename, i).getBytes();
                RawRecordContainerImpl value = getEvent(data);
                writer.append(key, value);
            }
        }
    }

    private void createRandomFiles(int length, int numFiles, Random random) throws IOException {
        Range[] ranges = createRanges(length, numFiles, random);

        for (int i = 0; i < numFiles; i++) {
            Path file = new Path(workDir, "test_" + i + ".seq");
            // create a file with length entries
            @SuppressWarnings("deprecation")
            SequenceFile.Writer writer = SequenceFile.createWriter(localFs, job.getConfiguration(), file, IntWritable.class, RawRecordContainerImpl.class);
            Range range = ranges[i];
            try {
                for (int j = range.start; j < range.end; j++) {
                    IntWritable key = new IntWritable(j);
                    byte[] data = new byte[random.nextInt(10)];
                    random.nextBytes(data);
                    RawRecordContainerImpl value = getEvent(data);
                    writer.append(key, value);
                }
            } finally {
                writer.close();
            }
        }
    }
}
