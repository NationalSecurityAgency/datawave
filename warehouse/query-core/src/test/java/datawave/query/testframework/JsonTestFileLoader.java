package datawave.query.testframework;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.json.config.helper.JsonIngestHelper;
import datawave.ingest.json.mr.input.JsonRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Loads test data from a Json input file. There are separate methods for loading data for Accumulo and for the processing of expected results.
 */
public class JsonTestFileLoader implements TestFileLoader, DataLoader {
    
    private static final Logger log = Logger.getLogger(JsonTestFileLoader.class);
    
    private final URI uri;
    private final Configuration conf;
    
    /**
     *
     * @param uri
     *            json file path contain test data
     * @param conf
     *            hadoop configuration for loading data
     */
    JsonTestFileLoader(URI uri, Configuration conf) {
        this.uri = uri;
        this.conf = conf;
    }
    
    @Override
    public void loadTestData(SequenceFile.Writer seqFile) throws IOException {
        TypeRegistry.reset();
        TypeRegistry.getInstance(this.conf);
        Path path = new Path(this.uri);
        File file = new File(this.uri);
        FileSplit split = new FileSplit(path, 0, file.length(), null);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(this.conf, new TaskAttemptID());
        
        try (JsonRecordReader reader = new JsonRecordReader()) {
            reader.initialize(split, ctx);
            while (reader.nextKeyValue()) {
                RawRecordContainer raw = reader.getEvent();
                seqFile.append(new Text(), raw);
            }
        }
    }
    
    @Override
    public Collection<Multimap<String,NormalizedContentInterface>> getRawData() throws IOException {
        TypeRegistry.reset();
        TypeRegistry.getInstance(this.conf);
        Path path = new Path(this.uri);
        File file = new File(this.uri);
        FileSplit split = new FileSplit(path, 0, file.length(), null);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(this.conf, new TaskAttemptID());
        
        JsonIngestHelper helper = new JsonIngestHelper();
        helper.setup(this.conf);
        
        List<Multimap<String,NormalizedContentInterface>> entries = new ArrayList<>();
        try (JsonRecordReader reader = new JsonRecordReader()) {
            reader.initialize(split, ctx);
            while (reader.nextKeyValue()) {
                RawRecordContainer record = reader.getEvent();
                Multimap<String,NormalizedContentInterface> fields = helper.getEventFields(record);
                entries.add(fields);
                if (log.isTraceEnabled()) {
                    for (Map.Entry<String,NormalizedContentInterface> entry : fields.entries()) {
                        log.trace("key(" + entry.getKey() + ") value(" + entry.getValue().getEventFieldValue() + ")");
                    }
                }
            }
        }
        
        log.debug("raw entries loaded: " + entries.size());
        
        return entries;
    }
}
