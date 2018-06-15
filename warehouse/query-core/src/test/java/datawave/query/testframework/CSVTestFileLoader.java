package datawave.query.testframework;

import datawave.ingest.csv.mr.input.CSVRecordReader;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Loads a test file for ingestion.
 */
public class CSVTestFileLoader implements TestFileLoader {
    
    private final URI uri;
    private final Configuration conf;
    private final RecordWriter<Text,Mutation> writer;
    
    CSVTestFileLoader(URI u, Configuration cf, RecordWriter<Text,Mutation> wr) {
        this.uri = u;
        this.conf = cf;
        this.writer = wr;
    }
    
    @Override
    public void loadTestData(SequenceFile.Writer seqFile) throws IOException {
        TypeRegistry.reset();
        TypeRegistry.getInstance(this.conf);
        Path path = new Path(this.uri);
        File file = new File(this.uri);
        FileSplit split = new FileSplit(path, 0, file.length(), null);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(this.conf, new TaskAttemptID());
        
        try (CSVRecordReader reader = new CSVRecordReader()) {
            reader.initialize(split, ctx);
            while (reader.nextKeyValue()) {
                RawRecordContainer raw = reader.getEvent();
                seqFile.append(new Text(), raw);
            }
        }
    }
    
}
