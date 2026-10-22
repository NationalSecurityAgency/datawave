package datawave.ingest.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.ingest.csv.mr.input.CSVReaderBase;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.policy.IngestPolicyEnforcer;

public class CSVReaderBaseTest {
    private static final String DATA_TYPE = "mycsv";
    private static final String ALL_CONFIG_FILE = "config/ingest/all-config.xml";
    private static final String CSV_CONFIG_FILE = "config/ingest/csv-ingest-config.xml";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Configuration conf;

    @Before
    public void setup() {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource(ALL_CONFIG_FILE));
        conf.addResource(ClassLoader.getSystemResource(CSV_CONFIG_FILE));
        conf.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DATA_TYPE + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, "");
        conf.set(DATA_TYPE + CSVHelper.DATA_HEADER, "VALUE");

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
    }

    @After
    public void tearDown() {
        TypeRegistry.reset();
    }

    @Test
    public void testSkipHeaderRowDoesNotSkipFirstRecordInNonFirstSplit() throws Exception {
        conf.set(DATA_TYPE + CSVHelper.SKIP_CSV_HEADER_ROW, "true");

        String data = "VALUE\nfirst\nsecond\n";
        File file = writeFile(data);
        long splitStart = data.indexOf("first") + 2;
        CSVReaderBase reader = initializeReader(file, splitStart, data.length() - splitStart);

        assertTrue(reader.nextKeyValue());
        assertEquals("second", reader.getCurrentValue().toString());
        assertFalse(reader.nextKeyValue());
    }

    @Test
    public void testDefaultTimestampUsesRawFileModificationTime() throws Exception {
        String data = "value\n";
        File file = writeFile(data);
        long fileModificationTime = 1_700_000_000_000L;
        assertTrue(file.setLastModified(fileModificationTime));

        CSVReaderBase reader = initializeReader(file, 0, data.length());
        assertTrue(reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();

        assertEquals(file.lastModified(), event.getTimestamp());
    }

    @Test
    public void testProgressIsZeroWhenTotalSizeIsZero() {
        CSVReaderBase reader = new CSVReaderBase();
        reader.setProcessedSize(1);
        reader.setTotalSize(0);

        assertEquals(0f, reader.getProgress(), 0f);
    }

    private File writeFile(String data) throws Exception {
        File file = temporaryFolder.newFile("input.csv");
        Files.write(file.toPath(), data.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private CSVReaderBase initializeReader(File file, long start, long length) throws Exception {
        CSVReaderBase reader = new CSVReaderBase();
        FileSplit split = new FileSplit(new Path(file.toURI().toString()), start, length, new String[0]);
        reader.initialize(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        return reader;
    }
}
